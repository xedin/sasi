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
package org.apache.cassandra.db.index.sasi.memory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.index.SSTableAttachedSecondaryIndex;
import org.apache.cassandra.db.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.db.index.sasi.disk.Token;
import org.apache.cassandra.db.index.sasi.plan.Expression;
import org.apache.cassandra.db.index.sasi.utils.RangeIterator;
import org.apache.cassandra.db.index.sasi.utils.TypeUtil;
import org.apache.cassandra.db.marshal.AbstractType;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.github.jamm.MemoryMeter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(IndexMemtable.class);

    private final MemoryMeter meter;

    private final ConcurrentMap<ByteBuffer, MemIndex> indexes;
    private final SSTableAttachedSecondaryIndex backend;

    public IndexMemtable(final SSTableAttachedSecondaryIndex backend)
    {
        this.indexes = new NonBlockingHashMap<>();
        this.backend = backend;
        this.meter = new MemoryMeter().omitSharedBufferOverhead().withTrackerProvider(new Callable<Set<Object>>()
        {
            public Set<Object> call() throws Exception
            {
                // avoid counting this once for each row
                Set<Object> set = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
                set.add(backend.getBaseCfs().metadata);
                return set;
            }
        });
    }

    public long estimateSize()
    {
        long deepSize = 0;
        for (MemIndex index : indexes.values())
            deepSize += index.estimateSize(meter);

        return deepSize;
    }

    public void index(ByteBuffer key, Iterator<Column> row)
    {
        final long now = System.currentTimeMillis();

        while (row.hasNext())
        {
            Column column = row.next();

            if (column.isMarkedForDelete(now))
                continue;

            ColumnIndex columnIndex = backend.getIndex(column.name());
            if (columnIndex == null)
                continue;

            final AbstractType<?> keyValidator = backend.getBaseCfs().metadata.getKeyValidator();

            MemIndex index = indexes.get(column.name());
            if (index == null)
            {
                MemIndex newIndex = MemIndex.forColumn(keyValidator, columnIndex);
                index = indexes.putIfAbsent(column.name(), newIndex);
                if (index == null)
                    index = newIndex;
            }

            ByteBuffer value = column.value();

            if (value.remaining() == 0)
                continue;

            if (!TypeUtil.isValid(value, columnIndex.getValidator()))
            {
                int size = value.remaining();
                if ((value = TypeUtil.tryUpcast(value, columnIndex.getValidator())) == null)
                {
                    logger.error("Can't add column {} to index for key: {}, value size {} bytes, validator: {}.",
                                 columnIndex.getColumnName(),
                                 keyValidator.getString(key),
                                 size,
                                 columnIndex.getValidator());
                    continue;
                }
            }

            index.add(value, key);
        }
    }

    public RangeIterator<Long, Token> search(Expression expression)
    {
        MemIndex index = indexes.get(expression.index.getDefinition().name);
        return index == null ? null : index.search(expression);
    }
}
