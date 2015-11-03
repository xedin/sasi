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
package org.apache.cassandra.db.index.sasi.disk;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.index.SSTableAttachedSecondaryIndex;
import org.apache.cassandra.db.index.sasi.utils.RangeIterator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableWriterListener;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PerSSTableIndexWriterTest extends SchemaLoader
{
    private static final String KS_NAME = "sasecondaryindex";
    private static final String CF_NAME = "saindexed1";

    @BeforeClass
    public static void loadSchema() throws IOException, ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        loadSchema(false);
    }

    @Test
    public void testPartialIndexWrites() throws Exception
    {
        final ByteBuffer column = UTF8Type.instance.decompose("age");
        final int maxKeys = 100000, numParts = 4, partSize = maxKeys / numParts;
        final String keyFormat = "key%06d";
        final long timestamp = System.currentTimeMillis();

        ColumnFamilyStore cfs = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        SSTableAttachedSecondaryIndex sasi = (SSTableAttachedSecondaryIndex) cfs.indexManager.getIndexForColumn(column);

        File directory = cfs.directories.getDirectoryForNewSSTables();
        Descriptor descriptor = Descriptor.fromFilename(cfs.getTempSSTablePath(directory));
        PerSSTableIndexWriter indexWriter = new PerSSTableIndexWriter(cfs.metadata.getKeyValidator(),
                                                                      descriptor,
                                                                      SSTableWriterListener.Source.MEMTABLE,
                                                                      sasi.getIndexes());

        SortedMap<DecoratedKey, Column> expectedKeys = new TreeMap<>(DecoratedKey.comparator);

        for (int i = 0; i < maxKeys; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.format(keyFormat, i));
            expectedKeys.put(StorageService.getPartitioner().decorateKey(key),
                     new Column(column, Int32Type.instance.decompose(i), timestamp));
        }

        indexWriter.begin();

        Iterator<Map.Entry<DecoratedKey, Column>> keyIterator = expectedKeys.entrySet().iterator();
        long position = 0;

        Set<String> segments = new HashSet<>();
        outer:
        for (;;)
        {
            for (int i = 0; i < partSize; i++)
            {
                if (!keyIterator.hasNext())
                    break outer;

                Map.Entry<DecoratedKey, Column> key = keyIterator.next();

                indexWriter.startRow(key.getKey(), position++);
                indexWriter.nextColumn(key.getValue());
            }

            PerSSTableIndexWriter.Index index = indexWriter.getIndex(column);

            OnDiskIndex segment = index.scheduleSegmentFlush(false).call();
            index.segments.add(Futures.immediateFuture(segment));
            segments.add(segment.getIndexPath());
        }

        for (String segment : segments)
            Assert.assertTrue(new File(segment).exists());

        String indexFile = indexWriter.indexes.get(column).filename(true);

        // final flush
        indexWriter.complete();

        for (String segment : segments)
            Assert.assertFalse(new File(segment).exists());

        OnDiskIndex index = new OnDiskIndex(new File(indexFile), Int32Type.instance, new Function<Long, DecoratedKey>()
        {
            @Override
            public DecoratedKey apply(Long position)
            {
                ByteBuffer key = ByteBufferUtil.bytes(String.format(keyFormat, position));
                return StorageService.getPartitioner().decorateKey(key);
            }
        });

        Assert.assertEquals(0, UTF8Type.instance.compare(index.minKey(), ByteBufferUtil.bytes(String.format(keyFormat, 0))));
        Assert.assertEquals(0, UTF8Type.instance.compare(index.maxKey(), ByteBufferUtil.bytes(String.format(keyFormat, maxKeys - 1))));

        Set<DecoratedKey> actualKeys = new HashSet<>();
        int count = 0;
        for (OnDiskIndex.DataTerm term : index)
        {
            RangeIterator<Long, Token> tokens = term.getTokens();

            while (tokens.hasNext())
            {
                for (DecoratedKey key : tokens.next())
                    actualKeys.add(key);
            }

            Assert.assertEquals(count++, (int) Int32Type.instance.compose(term.getTerm()));
        }

        Assert.assertEquals(expectedKeys.size(), actualKeys.size());
        for (DecoratedKey key : expectedKeys.keySet())
            Assert.assertTrue(actualKeys.contains(key));

        FileUtils.closeQuietly(index);
    }
}
