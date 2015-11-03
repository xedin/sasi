/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.index.sasi.plan;

import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.SSTableAttachedSecondaryIndex;
import org.apache.cassandra.db.index.sasi.plan.Operation.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.IndexExpression;

public class QueryPlan
{
    /**
     * A sanity ceiling on the number of max rows we'll ever return for a query.
     */
    private static final int MAX_ROWS = 10000;

    private final SSTableAttachedSecondaryIndex backend;
    private final ExtendedFilter filter;

    private final QueryController controller;

    public QueryPlan(SSTableAttachedSecondaryIndex backend, ExtendedFilter filter, long executionQuotaMs)
    {
        this.backend = backend;
        this.filter = filter;
        this.controller = new QueryController(backend, filter, executionQuotaMs);
    }

    /**
     * Converts list of IndexExpressions' in polish notation into
     * operation tree preserving priority.
     *
     * Operation tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * satisfies by checks for resulting rows with an early exit.
     *
     * @return root of the operations tree.
     */
    private Operation analyze()
    {
        AbstractType<?> comparator = backend.getBaseCfs().getComparator();

        Stack<Operation.Builder> operations = new Stack<>();
        Stack<IndexExpression> group = new Stack<>();

        boolean isOldFormat = true;
        for (IndexExpression e : filter.getClause())
        {
            if (e.isSetLogicalOp())
            {
                OperationType op = OperationType.valueOf(e.logicalOp.name());

                Operation.Builder sideL, sideR;

                switch (group.size())
                {
                    case 0: // both arguments come from the operations stack
                        sideL = operations.pop();
                        sideR = operations.pop();

                        Operation.Builder operation = new Operation.Builder(op, comparator, controller);

                        /**
                         * If both underlying operations are the same we can improve performance
                         * and decrease work set size by combining operations under one AND/OR
                         * instead of making a tree, example:
                         *
                         *         AND                                 AND
                         *       /     \                            /   |   \
                         *     AND     AND              =>      name  age   height
                         *    /   \       \
                         * name:p  age:20  height:180
                         */
                        if (sideL.op == sideR.op && op == sideL.op)
                        {
                            operation.add(sideL.expressions);
                            operation.add(sideR.expressions);
                        }
                        else
                        {
                            operation.setLeft(sideL).setRight(sideR);
                        }

                        operations.push(operation);
                        break;

                    case 1: // one of the arguments is from group other is from already constructed operations stack
                        sideR = operations.pop();

                        /**
                         * As all of the AND operators are of the same priority
                         * we can do an optimization that gives us a lot of benefits on the execution side.
                         *
                         * We try to detect situation where two or more AND operations are linked together
                         * in the tree and pull all of their expressions up to the first seen AND statement,
                         * that helps expression analyzer to figure out proper bounds for the same field,
                         * as well as reduces number of SSTableIndex files we have to use to satisfy the
                         * query by means of primary expression analysis.
                         *
                         *         AND            AND              AND
                         *        /   \         /  |  \          /     \
                         *      AND   a<7  => f:x a>5 a<7  =>  f:x  5 < a < 7
                         *     /   \
                         *   f:x    a>5
                         */
                        if (op == OperationType.AND && op == sideR.op)
                        {
                            sideR.add(group.pop());
                            operations.push(sideR);
                        }
                        else
                        {
                            operation = new Operation.Builder(op, comparator, controller, group.pop());
                            operation.setRight(sideR);

                            operations.push(operation);
                        }

                        break;

                    default: // all arguments come from group expressions
                        operations.push(new Operation.Builder(op, comparator, controller, group.pop(), group.pop()));
                        break;
                }

                isOldFormat = false;
            }
            else
            {
                group.add(e);
            }
        }

        // we need to support old IndexExpression format without
        // logical expressions, so we assume that all of the columns
        // are linked with AND.
        if (isOldFormat)
        {
            Operation.Builder and = new Operation.Builder(OperationType.AND, comparator, controller);

            while (!group.empty())
                and.add(group.pop());

            operations.push(and);
        }

        Operation.Builder root = operations.pop();
        return root.complete();
    }

    public List<Row> execute() throws RequestTimeoutException
    {
        DataRange range = filter.dataRange;
        RowPosition lastKey = range.stopKey();

        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();

        final int maxRows = Math.min(filter.maxColumns(), Math.min(MAX_ROWS, filter.maxRows()));
        final List<Row> rows = new ArrayList<>(maxRows);

        Operation operationTree = null;

        try
        {
            operationTree = analyze();

            if (operationTree == null)
                return Collections.emptyList();

            operationTree.skipTo(((LongToken) range.keyRange().left.getToken()).token);

            intersection:
            while (operationTree.hasNext())
            {
                for (DecoratedKey key : operationTree.next())
                {
                    if ((!lastKey.isMinimum(partitioner) && lastKey.compareTo(key) < 0) || rows.size() >= maxRows)
                        break intersection;

                    if (!range.contains(key))
                        continue;

                    Row row = getRow(key, filter);
                    if (row != null && operationTree.satisfiedBy(row, null, !isColumnSlice(key)))
                        rows.add(row);
                }
            }
        }
        finally
        {
            FileUtils.closeQuietly(operationTree);
            controller.finish();
        }

        return rows;
    }

    private Row getRow(DecoratedKey key, ExtendedFilter filter)
    {
        try
        {
            ColumnFamilyStore store = backend.getBaseCfs();
            ReadCommand cmd = ReadCommand.create(store.metadata.ksName,
                                                 key.key,
                                                 store.metadata.cfName,
                                                 System.currentTimeMillis(),
                                                 filter.columnFilter(key.key));

            return cmd.getRow(store.keyspace);
        }
        finally
        {
            controller.checkpoint();
        }
    }

    private boolean isColumnSlice(DecoratedKey key)
    {
        return filter.columnFilter(key.key) instanceof SliceQueryFilter;
    }
}
