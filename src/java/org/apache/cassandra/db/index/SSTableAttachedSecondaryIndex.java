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
package org.apache.cassandra.db.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.SSTableSliceIterator;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.db.index.sasi.disk.PerSSTableIndexWriter;
import org.apache.cassandra.db.index.sasi.exceptions.TimeQuotaExceededException;
import org.apache.cassandra.db.index.sasi.memory.IndexMemtable;
import org.apache.cassandra.db.index.sasi.metrics.IndexMetrics;
import org.apache.cassandra.db.index.sasi.plan.QueryPlan;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.SSTableWriterListener.Source;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.notifications.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Note: currently does not work with cql3 tables (unless 'WITH COMPACT STORAGE' is declared when creating the table).
 *
 * ALos, makes the assumption this will be the only index running on the table as part of the query.
 * SIM tends to shoves all indexed columns into one PerRowSecondaryIndex
 */
public class SSTableAttachedSecondaryIndex extends PerRowSecondaryIndex implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableAttachedSecondaryIndex.class);

    private IndexMetrics metrics;

    private final ConcurrentMap<ByteBuffer, ColumnIndex> indexedColumns;
    private final AtomicReference<IndexMemtable> globalMemtable = new AtomicReference<>(new IndexMemtable(this));

    private AbstractType<?> keyValidator;
    private boolean isInitialized;

    public SSTableAttachedSecondaryIndex()
    {
        indexedColumns = new NonBlockingHashMap<>();
    }

    public void init()
    {
        if (!(StorageService.getPartitioner() instanceof Murmur3Partitioner))
            throw new UnsupportedOperationException("SASI supported only with Murmur3Partitioner.");

        isInitialized = true;

        metrics = new IndexMetrics(baseCfs);

        // init() is called by SIM only on the instance that it will keep around, but will call addColumnDef on any instance
        // that it happens to create (and subsequently/immediately throw away)
        track(columnDefs);

        baseCfs.getDataTracker().subscribe(this);
    }

    @VisibleForTesting
    public void addColumnDef(ColumnDefinition columnDef)
    {
        super.addColumnDef(columnDef);
        track(Collections.singleton(columnDef));
    }

    private void track(Set<ColumnDefinition> columns)
    {
        // if SI hasn't been initialized that means that this instance
        // was created for validation purposes only, so we don't have do anything here.
        // And only reason baseCfs would be null is if coming through the CFMetaData.validate() path, which only
        // checks that the SI 'looks' legit, but then throws away any created instances - fml, this 2I api sux
        if (!isInitialized || baseCfs == null)
            return;

        if (keyValidator == null)
            keyValidator = baseCfs.metadata.getKeyValidator();

        Set<SSTableReader> sstables = baseCfs.getDataTracker().getSSTables();
        NavigableMap<SSTableReader, Map<ByteBuffer, ColumnIndex>> toRebuild = new TreeMap<>(new Comparator<SSTableReader>()
        {
            @Override
            public int compare(SSTableReader a, SSTableReader b)
            {
                return Integer.compare(a.descriptor.generation, b.descriptor.generation);
            }
        });

        for (ColumnDefinition column : columns)
        {
            ColumnIndex index = indexedColumns.get(column.name);
            if (index == null)
            {
                ColumnIndex newIndex = new ColumnIndex(keyValidator, column, getComparator(column));
                index = indexedColumns.putIfAbsent(column.name, newIndex);

                if (index != null)
                    continue;

                // on restart, sstables are loaded into DataTracker before 2I are hooked up (and init() invoked),
                // so we need to explicitly load sstables per index.
                for (SSTableReader sstable : newIndex.init(sstables))
                {
                    Map<ByteBuffer, ColumnIndex> perSSTable = toRebuild.get(sstable);
                    if (perSSTable == null)
                        toRebuild.put(sstable, (perSSTable = new HashMap<>()));

                    perSSTable.put(column.name, newIndex);
                }
            }
        }

        // schedule rebuild of the missing indexes. Property affects both existing and newly (re-)created
        // indexes since SecondaryIndex API makes no distinction between them.
        if (Boolean.parseBoolean(System.getProperty("cassandra.sasi.rebuild_on_start", "true")))
            CompactionManager.instance.submitIndexBuild(new IndexBuilder(toRebuild));
    }

    public AbstractType<?> getKeyValidator()
    {
        return keyValidator;
    }

    public AbstractType<?> getComparator(ColumnDefinition column)
    {
        return baseCfs.metadata.getColumnDefinitionComparator(column);
    }

    public Map<ByteBuffer, ColumnIndex> getIndexes()
    {
        return indexedColumns;
    }

    public ColumnIndex getIndex(ByteBuffer columnName)
    {
        return indexedColumns.get(columnName);
    }

    private void updateView(Collection<SSTableReader> toRemove, Collection<SSTableReader> toAdd, Collection<ColumnDefinition> columns)
    {
        for (ColumnDefinition column : columns)
        {
            ColumnIndex columnIndex = indexedColumns.get(column.name);
            if (columnIndex == null)
            {
                track(Collections.singleton(column));
                continue;
            }

            columnIndex.update(toRemove, toAdd);
        }
    }

    public boolean isIndexBuilt(ByteBuffer columnName)
    {
        return true;
    }

    public void validateOptions() throws ConfigurationException
    {
        for (ColumnIndex index : indexedColumns.values())
            index.validate();
    }

    public String getIndexName()
    {
        return "RowLevel_SASI_" + baseCfs.getColumnFamilyName();
    }

    public ColumnFamilyStore getIndexCfs()
    {
        return null;
    }

    public long getLiveSize()
    {
        return globalMemtable.get().estimateSize();
    }

    public void reload()
    {
        invalidateMemtable();
    }

    public void index(ByteBuffer key, ColumnFamily cf)
    {
        // avoid adding already expired or deleted columns to the index
        if (cf == null || cf.isMarkedForDelete())
            return;

        globalMemtable.get().index(key, cf.iterator());
    }

    /**
     * parent class to eliminate the index rebuild
     *
     * @return a future that does and blocks on nothing
     */
    public Future<?> buildIndexAsync()
    {
        return Futures.immediateCheckedFuture(null);
    }

    @Override
    public void buildIndexes(Collection<SSTableReader> sstablesToRebuild, Set<String> indexNames)
    {
        NavigableMap<SSTableReader, Map<ByteBuffer, ColumnIndex>> sstables = new TreeMap<>(new Comparator<SSTableReader>()
        {
            @Override
            public int compare(SSTableReader a, SSTableReader b)
            {
                return Integer.compare(a.descriptor.generation, b.descriptor.generation);
            }
        });

        Map<ByteBuffer, ColumnIndex> indexes = new HashMap<>();
        for (ColumnIndex index : indexedColumns.values())
        {
            Iterator<String> iterator = indexNames.iterator();

            while (iterator.hasNext())
            {
                String indexName = iterator.next();
                if (index.getIndexName().equals(indexName))
                {
                    index.dropData(FBUtilities.timestampMicros());

                    ColumnDefinition indexToBuild = index.getDefinition();
                    indexes.put(indexToBuild.name, index);
                    iterator.remove();
                    break;
                }
            }
        }

        if (indexes.isEmpty())
            return;

        for (SSTableReader sstable : sstablesToRebuild)
            sstables.put(sstable, indexes);

        try
        {
            FBUtilities.waitOnFuture(CompactionManager.instance.submitIndexBuild(new IndexBuilder(sstables)));
        }
        catch (Exception e)
        {
            logger.error("Failed index build task", e);
        }
    }

    public void delete(DecoratedKey key)
    {
        // called during 'nodetool cleanup' - can punt on impl'ing this
    }

    public void removeIndex(ByteBuffer columnName)
    {
        ColumnIndex index = indexedColumns.remove(columnName);
        if (index != null)
            index.dropData(FBUtilities.timestampMicros());
    }

    public void invalidate()
    {
        invalidate(true, FBUtilities.timestampMicros());
    }

    public void invalidate(boolean invalidateMemtable, long truncateUntil)
    {
        if (invalidateMemtable)
            invalidateMemtable();

        for (ColumnIndex index : indexedColumns.values())
            index.dropData(truncateUntil);
    }

    private void invalidateMemtable()
    {
        globalMemtable.getAndSet(new IndexMemtable(this));
    }

    public void truncateBlocking(long truncatedAt)
    {
        invalidate(false, truncatedAt);
    }

    public void forceBlockingFlush()
    {
        //nop, as this 2I will flush with the owning CF's sstable, so we don't need this extra work
    }

    public Collection<Component> getIndexComponents()
    {
        ImmutableList.Builder<Component> components = ImmutableList.builder();
        for (ColumnIndex index : indexedColumns.values())
            components.add(index.getComponent());

        return components.build();
    }

    public SSTableWriterListener getWriterListener(Descriptor descriptor, Source source)
    {
        return newWriter(descriptor, indexedColumns, source);
    }

    public IndexMemtable getMemtable()
    {
        return globalMemtable.get();
    }

    protected PerSSTableIndexWriter newWriter(Descriptor descriptor, Map<ByteBuffer, ColumnIndex> indexes, Source source)
    {
        return new PerSSTableIndexWriter(keyValidator, descriptor, source, indexes);
    }

    public void handleNotification(INotification notification, Object sender)
    {
        // unfortunately, we can only check the type of notification via instanceof :(
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification notif = (SSTableAddedNotification) notification;
            updateView(Collections.<SSTableReader>emptyList(), Collections.singletonList(notif.added), getColumnDefs());
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification notif = (SSTableListChangedNotification) notification;
            updateView(notif.removed, notif.added, getColumnDefs());
        }
        else if (notification instanceof MemtableRenewedNotification)
        {
            invalidateMemtable();
        }
    }

    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
    {
        return new LocalSecondaryIndexSearcher(baseCfs.indexManager, columns);
    }

    protected class LocalSecondaryIndexSearcher extends SecondaryIndexSearcher
    {
        protected LocalSecondaryIndexSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
        {
            super(indexManager, columns);
        }

        public List<Row> search(ExtendedFilter filter)
        {
            metrics.markRequest();

            long startTime = System.nanoTime();

            try
            {
                return filter != null && !filter.getClause().isEmpty()
                        ? new QueryPlan(SSTableAttachedSecondaryIndex.this, filter, DatabaseDescriptor.getRangeRpcTimeout()).execute()
                        : Collections.<Row>emptyList();
            }
            catch (Exception e)
            {
                metrics.markFailure();

                if (e instanceof TimeQuotaExceededException)
                    metrics.markTimeout();
                else
                    logger.warn("error occurred while searching SASI indexes; ignoring", e);

                return Collections.emptyList();
            }
            finally
            {
                metrics.updateLatency(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            }
        }

        public boolean isIndexing(List<IndexExpression> clause)
        {
            // this is a bit weak, currently just checks for the success of one column, not all
            // however, parent SIS.isIndexing only cares if one predicate is covered ... grrrr!!
            for (IndexExpression expression : clause)
            {
                if (indexedColumns.containsKey(expression.column_name))
                    return true;
            }
            return false;
        }
    }

    private class IndexBuilder extends IndexBuildTask
    {
        private final SortedMap<SSTableReader, Map<ByteBuffer, ColumnIndex>> sstables;

        private long bytesProcessed = 0;
        private final long totalSizeInBytes;

        public IndexBuilder(SortedMap<SSTableReader, Map<ByteBuffer, ColumnIndex>> sstables)
        {
            long totalIndexBytes = 0;
            for (SSTableReader sstable : sstables.keySet())
                totalIndexBytes += getPrimaryIndexLength(sstable);

            this.sstables = sstables;
            this.totalSizeInBytes = totalIndexBytes;
        }

        @Override
        public void build()
        {
            for (Map.Entry<SSTableReader, Map<ByteBuffer, ColumnIndex>> e : sstables.entrySet())
            {
                SSTableReader sstable = e.getKey();
                Map<ByteBuffer, ColumnIndex> indexes = e.getValue();

                if (!sstable.acquireReference())
                {
                    bytesProcessed += getPrimaryIndexLength(sstable);
                    continue;
                }

                try
                {
                    PerSSTableIndexWriter indexWriter = newWriter(sstable.descriptor.asTemporary(true), indexes, Source.COMPACTION);

                    long previousKeyPosition = 0;
                    try (KeyIterator keys = new KeyIterator(sstable.descriptor))
                    {
                        while (keys.hasNext())
                        {
                            if (isStopRequested())
                                throw new CompactionInterruptedException(getCompactionInfo());

                            final DecoratedKey key = keys.next();
                            final long keyPosition = keys.getKeyPosition();

                            indexWriter.startRow(key, keyPosition);
                            try (SSTableSliceIterator row = new SSTableSliceIterator(sstable, key, ColumnSlice.ALL_COLUMNS_ARRAY, false))
                            {
                                while (row.hasNext())
                                {
                                    OnDiskAtom atom = row.next();
                                    if (atom != null && atom instanceof Column)
                                        indexWriter.nextColumn((Column) atom);
                                }
                            }
                            catch (IOException ex)
                            {
                                throw new FSReadError(ex, sstable.getFilename());
                            }

                            bytesProcessed += keyPosition - previousKeyPosition;
                            previousKeyPosition = keyPosition;
                        }

                        completeSSTable(indexWriter, sstable, indexes.values());
                    }
                }
                finally
                {
                    sstable.releaseReference();
                }
            }
        }

        @Override
        public CompactionInfo getCompactionInfo()
        {
            return new CompactionInfo(baseCfs.metadata, OperationType.INDEX_BUILD, bytesProcessed, totalSizeInBytes);
        }

        private long getPrimaryIndexLength(SSTable sstable)
        {
            File primaryIndex = new File(sstable.getIndexFilename());
            return primaryIndex.exists() ? primaryIndex.length() : 0;
        }

        private void completeSSTable(PerSSTableIndexWriter indexWriter, SSTableReader sstable, Collection<ColumnIndex> indexes)
        {
            indexWriter.complete();

            for (ColumnIndex index : indexes)
            {
                File tmpIndex = new File(index.getPath(indexWriter.getDescriptor()));
                if (!tmpIndex.exists()) // no data was inserted into the index for given sstable
                    continue;

                FileUtils.renameWithConfirm(tmpIndex, new File(index.getPath(sstable.descriptor)));
                index.update(Collections.<SSTableReader>emptyList(), Collections.singletonList(sstable));
            }
        }
    }
}
