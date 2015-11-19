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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.SSTableAttachedSecondaryIndex;
import org.apache.cassandra.db.index.sasi.plan.Operation.OperationType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;

import org.apache.cassandra.thrift.LogicalIndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.*;

public class OperationTest extends SchemaLoader
{
    private static SSTableAttachedSecondaryIndex BACKEND;

    @BeforeClass
    public static void loadSchema() throws IOException, ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        loadSchema(false);

        ColumnFamilyStore store = Keyspace.open("sasecondaryindex").getColumnFamilyStore("saindexed1");
        BACKEND = (SSTableAttachedSecondaryIndex) store.indexManager.getIndexForColumn(UTF8Type.instance.decompose("first_name"));
    }

    private QueryController controller;

    @Before
    public void beforeTest()
    {
        ExtendedFilter filter = ExtendedFilter.create(BACKEND.getBaseCfs(),
                                                      DataRange.allData(new Murmur3Partitioner()),
                                                      null,
                                                      Integer.MAX_VALUE,
                                                      false,
                                                      System.currentTimeMillis());

        controller = new QueryController(BACKEND, filter, TimeUnit.SECONDS.toMillis(10));
    }

    @After
    public void afterTest()
    {
        controller.finish();
    }

    @Test
    public void testAnalyze() throws Exception
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");
        final ByteBuffer comment = UTF8Type.instance.decompose("comment");

        // age != 5 AND age > 1 AND age != 6 AND age <= 10
        Map<Expression.Op, Expression> expressions = convert(Operation.analyzeGroup(controller, UTF8Type.instance,OperationType.AND,
                                                                                Arrays.asList(new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)),
                                                                                              new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(1)),
                                                                                              new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(6)),
                                                                                              new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(10)))));

        Expression expected = new Expression(age, Int32Type.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(Int32Type.instance.decompose(1), false);
            upper = new Bound(Int32Type.instance.decompose(10), true);

            exclusions.add(Int32Type.instance.decompose(5));
            exclusions.add(Int32Type.instance.decompose(6));
        }};

        Assert.assertEquals(1, expressions.size());
        Assert.assertEquals(expected, expressions.get(Expression.Op.RANGE));

        // age != 5 OR age >= 7
        expressions = convert(Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.OR,
                        Arrays.asList(new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)),
                                      new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(7)))));
        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression(age, Int32Type.instance)
                            {{
                                    operation = Op.NOT_EQ;
                                    lower = new Bound(Int32Type.instance.decompose(5), true);
                                    upper = lower;
                            }}, expressions.get(Expression.Op.NOT_EQ));

        Assert.assertEquals(new Expression(age, Int32Type.instance)
                            {{
                                    operation = Op.RANGE;
                                    lower = new Bound(Int32Type.instance.decompose(7), true);
                            }}, expressions.get(Expression.Op.RANGE));

        // age != 5 OR age < 7
        expressions = convert(Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.OR,
                        Arrays.asList(new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)),
                                      new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(7)))));

        Assert.assertEquals(2, expressions.size());
        Assert.assertEquals(new Expression(age, Int32Type.instance)
                            {{
                                    operation = Op.RANGE;
                                    upper = new Bound(Int32Type.instance.decompose(7), false);
                            }}, expressions.get(Expression.Op.RANGE));
        Assert.assertEquals(new Expression(age, Int32Type.instance)
                            {{
                                    operation = Op.NOT_EQ;
                                    lower = new Bound(Int32Type.instance.decompose(5), true);
                                    upper = lower;
                            }}, expressions.get(Expression.Op.NOT_EQ));

        // age > 1 AND age < 7
        expressions = convert(Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.AND,
                        Arrays.asList(new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(1)),
                                      new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(7)))));

        Assert.assertEquals(1, expressions.size());
        Assert.assertEquals(new Expression(age, Int32Type.instance)
                            {{
                                    operation = Op.RANGE;
                                    lower = new Bound(Int32Type.instance.decompose(1), false);
                                    upper = new Bound(Int32Type.instance.decompose(7), false);
                            }}, expressions.get(Expression.Op.RANGE));

        // first_name = 'a' OR first_name != 'b'
        expressions = convert(Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.OR,
                        Arrays.asList(new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                      new IndexExpression(firstName, IndexOperator.NOT_EQ, UTF8Type.instance.decompose("b")))));

        Assert.assertEquals(2, expressions.size());
        Assert.assertEquals(new Expression(firstName, UTF8Type.instance)
                            {{
                                    operation = Op.NOT_EQ;
                                    lower = new Bound(UTF8Type.instance.decompose("b"), true);
                                    upper = lower;
                            }}, expressions.get(Expression.Op.NOT_EQ));
        Assert.assertEquals(new Expression(firstName, UTF8Type.instance)
                            {{
                                    operation = Op.EQ;
                                    lower = upper = new Bound(UTF8Type.instance.decompose("a"), true);
                            }}, expressions.get(Expression.Op.EQ));

        // comment = 'soft eng' and comment != 'likes do'
        ListMultimap<ByteBuffer, Expression> e = Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.OR,
                                                    Arrays.asList(new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("soft eng")),
                                                                  new IndexExpression(comment, IndexOperator.NOT_EQ, UTF8Type.instance.decompose("likes do"))));

        List<Expression> expectedExpressions = new ArrayList<Expression>(2)
        {{
                add(new Expression(comment, UTF8Type.instance)
                {{
                        operation = Op.EQ;
                        lower = new Bound(UTF8Type.instance.decompose("soft"), true);
                        upper = lower;
                }});

                add(new Expression(comment, UTF8Type.instance)
                {{
                        operation = Op.EQ;
                        lower = new Bound(UTF8Type.instance.decompose("eng"), true);
                        upper = lower;
                }});

                add(new Expression(comment, UTF8Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("likes"), true);
                        upper = lower;
                }});

                add(new Expression(comment, UTF8Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("do"), true);
                        upper = lower;
                }});
        }};

        Assert.assertEquals(expectedExpressions, e.get(comment));

        // first_name = 'j' and comment != 'likes do'
        e = Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.OR,
                        Arrays.asList(new IndexExpression(comment, IndexOperator.NOT_EQ, UTF8Type.instance.decompose("likes do")),
                                      new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("j"))));

        expectedExpressions = new ArrayList<Expression>(2)
        {{
                add(new Expression(comment, UTF8Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("likes"), true);
                        upper = lower;
                }});

                add(new Expression(comment, UTF8Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("do"), true);
                        upper = lower;
                }});
        }};

        Assert.assertEquals(expectedExpressions, e.get(comment));

        // age != 27 first_name = 'j' and age != 25
        e = Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.OR,
                        Arrays.asList(new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(27)),
                                      new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("j")),
                                      new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(25))));

        expectedExpressions = new ArrayList<Expression>(2)
        {{
                add(new Expression(age, Int32Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(Int32Type.instance.decompose(27), true);
                        upper = lower;
                }});

                add(new Expression(age, Int32Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(Int32Type.instance.decompose(25), true);
                        upper = lower;
                }});
        }};

        Assert.assertEquals(expectedExpressions, e.get(age));
    }

    @Test
    public void testSatisfiedBy() throws Exception
    {
        final ByteBuffer timestamp = UTF8Type.instance.decompose("timestamp");
        final ByteBuffer age = UTF8Type.instance.decompose("age");
        final IPartitioner<?> partitioner = StorageService.getPartitioner();

        ColumnFamilyStore store = Keyspace.open("sasecondaryindex").getColumnFamilyStore("saindexed1");

        Operation.Builder builder = new Operation.Builder(OperationType.AND, UTF8Type.instance, controller, new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)));
        Operation op = builder.complete();

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(5), System.currentTimeMillis()));

        // and reject incorrect value
        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        // range with exclusions - age != 5 AND age > 1 AND age != 6 AND age <= 10
        builder = new Operation.Builder(OperationType.AND, UTF8Type.instance, controller,
                                                                 new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)),
                                                                 new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(1)),
                                                                 new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(6)),
                                                                 new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(10)));
        op = builder.complete();

        Set<Integer> exclusions = Sets.newHashSet(0, 1, 5, 6, 11);
        for (int i = 0; i <= 11; i++)
        {
            cf = ArrayBackedSortedColumns.factory.create(store.metadata);
            cf.addColumn(new Column(age, Int32Type.instance.decompose(i), System.currentTimeMillis()));

            boolean result = op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false);
            Assert.assertTrue(exclusions.contains(i) ? !result : result);
        }

        // now let's do something more complex - age = 5 OR age = 6
        builder = new Operation.Builder(OperationType.OR, UTF8Type.instance, controller,
                                            new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(5)),
                                            new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(6)));

        op = builder.complete();

        exclusions = Sets.newHashSet(0, 1, 2, 3, 4, 7, 8, 9, 10);
        for (int i = 0; i <= 10; i++)
        {
            cf = ArrayBackedSortedColumns.factory.create(store.metadata);
            cf.addColumn(new Column(age, Int32Type.instance.decompose(i), System.currentTimeMillis()));

            boolean result = op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false);
            Assert.assertTrue(exclusions.contains(i) ? !result : result);
        }

        // now let's test aggregated AND commands
        builder = new Operation.Builder(OperationType.AND, UTF8Type.instance, controller);

        // logical should be ignored by analyzer, but we still what to make sure that it is
        IndexExpression logical = new IndexExpression(ByteBufferUtil.EMPTY_BYTE_BUFFER, IndexOperator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        logical.setLogicalOp(LogicalIndexOperator.AND);

        builder.add(logical);
        builder.add(new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(0)));
        builder.add(new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(10)));
        builder.add(new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(7)));

        op = builder.complete();

        exclusions = Sets.newHashSet(7);
        for (int i = 0; i < 10; i++)
        {
            cf = ArrayBackedSortedColumns.factory.create(store.metadata);
            cf.addColumn(new Column(age, Int32Type.instance.decompose(i), System.currentTimeMillis()));

            boolean result = op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false);
            Assert.assertTrue(exclusions.contains(i) ? !result : result);
        }

        // multiple analyzed expressions in the Operation timestamp >= 10 AND age = 5
        builder = new Operation.Builder(OperationType.AND, UTF8Type.instance, controller);
        builder.add(new IndexExpression(timestamp, IndexOperator.GTE, LongType.instance.decompose(10L)));
        builder.add(new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(5)));

        op = builder.complete();

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(11L), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(5), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(22L), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(5), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        // operation with internal expressions and right child
        builder = new Operation.Builder(OperationType.OR, UTF8Type.instance, controller,
                                                                        new IndexExpression(timestamp, IndexOperator.GT, LongType.instance.decompose(10L)));
        builder.setRight(new Operation.Builder(OperationType.AND, UTF8Type.instance, controller,
                                                                        new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(0)),
                                                                        new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(10))));
        op = builder.complete();

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(5), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(20), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(11L), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(age, Int32Type.instance.decompose(0), System.currentTimeMillis()));
        cf.addColumn(new Column(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        // and for desert let's try out null and deleted rows etc.
        builder = new Operation.Builder(OperationType.AND, UTF8Type.instance, controller);
        builder.add(new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(30)));
        op = builder.complete();

        Assert.assertFalse(op.satisfiedBy(null, null, false));
        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), null), null, false));

        long now = System.currentTimeMillis();

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.setDeletionInfo(new DeletionInfo(now - 10, (int) (now / 1000)));
        cf.addColumn(new Column(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new DeletedColumn(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, true));

        try
        {
            cf = ArrayBackedSortedColumns.factory.create(store.metadata);
            Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));
        }
        catch (IllegalStateException e)
        {
            // expected
        }

        try
        {
            cf = ArrayBackedSortedColumns.factory.create(store.metadata);
            Assert.assertFalse(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, true));
        }
        catch (IllegalStateException e)
        {
            Assert.fail("IllegalStateException should not be thrown when missing column and allowMissingColumns=true");
        }
    }

    @Test
    public void testAnalyzeNotIndexedButDefinedColumn() throws Exception
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer height = UTF8Type.instance.decompose("height");
        final ByteBuffer notDefined = UTF8Type.instance.decompose("not-defined");

        // first_name = 'a' AND height != 10
        Map<Expression.Op, Expression> expressions;
        expressions = convert(Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.AND,
                Arrays.asList(new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                              new IndexExpression(height, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)))));

        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression(height, Int32Type.instance)
        {{
                operation = Op.NOT_EQ;
                lower = new Bound(Int32Type.instance.decompose(5), true);
                upper = lower;
        }}, expressions.get(Expression.Op.NOT_EQ));

        expressions = convert(Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.AND,
                Arrays.asList(new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                              new IndexExpression(height, IndexOperator.GT, Int32Type.instance.decompose(0)),
                              new IndexExpression(height, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)))));

        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression(height, Int32Type.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(Int32Type.instance.decompose(0), false);
            exclusions.add(Int32Type.instance.decompose(5));
        }}, expressions.get(Expression.Op.RANGE));

        expressions = convert(Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.AND,
                Arrays.asList(new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                              new IndexExpression(height, IndexOperator.NOT_EQ, Int32Type.instance.decompose(5)),
                              new IndexExpression(height, IndexOperator.GTE, Int32Type.instance.decompose(0)),
                              new IndexExpression(height, IndexOperator.LT, Int32Type.instance.decompose(10)))));

        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression(height, Int32Type.instance)
        {{
                operation = Op.RANGE;
                lower = new Bound(Int32Type.instance.decompose(0), true);
                upper = new Bound(Int32Type.instance.decompose(10), false);
                exclusions.add(Int32Type.instance.decompose(5));
        }}, expressions.get(Expression.Op.RANGE));


        expressions = convert(Operation.analyzeGroup(controller, UTF8Type.instance, OperationType.AND,
                        new ArrayList<IndexExpression>() {{ add(new IndexExpression(notDefined, IndexOperator.EQ, UTF8Type.instance.decompose("a"))); }}));

        Assert.assertEquals(1, expressions.size());
    }

    @Test
    public void testSatisfiedByWithMultipleTerms()
    {
        final ByteBuffer comment = UTF8Type.instance.decompose("comment");
        final ColumnFamilyStore store = Keyspace.open("sasecondaryindex").getColumnFamilyStore("saindexed1");
        final IPartitioner<?> partitioner = StorageService.getPartitioner();

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(store.metadata);
        cf.addColumn(new Column(comment, UTF8Type.instance.decompose("software engineer is working on a project"), System.currentTimeMillis()));

        Operation.Builder builder = new Operation.Builder(OperationType.AND, UTF8Type.instance, controller,
                                            new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("eng is a work")));
        Operation op = builder.complete();

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));

        builder = new Operation.Builder(OperationType.AND, UTF8Type.instance, controller,
                                            new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("soft works fine")));
        op = builder.complete();

        Assert.assertTrue(op.satisfiedBy(new Row(partitioner.decorateKey(UTF8Type.instance.decompose("key1")), cf), null, false));
    }

    private Map<Expression.Op, Expression> convert(Multimap<ByteBuffer, Expression> expressions)
    {
        Map<Expression.Op, Expression> converted = new HashMap<>();
        for (Expression expression : expressions.values())
        {
            Expression column = converted.get(expression.getOp());
            assert column == null; // sanity check
            converted.put(expression.getOp(), expression);
        }

        return converted;
    }
}
