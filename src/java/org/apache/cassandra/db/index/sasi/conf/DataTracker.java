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
package org.apache.cassandra.db.index.sasi.conf;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.index.sasi.SSTableIndex;
import org.apache.cassandra.db.index.sasi.conf.view.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** a pared-down version of DataTracker and DT.View. need one for each index of each column family */
public class DataTracker
{
    private static final Logger logger = LoggerFactory.getLogger(DataTracker.class);

    private final AbstractType<?> keyValidator;
    private final ColumnIndex columnIndex;
    private final AtomicReference<View> view = new AtomicReference<>();

    public DataTracker(AbstractType<?> keyValidator, ColumnIndex index, Set<SSTableReader> sstables)
    {
        this.keyValidator = keyValidator;
        this.columnIndex = index;
        this.view.set(new View(index, keyValidator, getIndexes(sstables)));
    }

    public View getView()
    {
        return view.get();
    }

    public void update(Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newSSTables)
    {
        Set<SSTableIndex> newIndexes = getIndexes(newSSTables);

        View currentView, newView;
        do
        {
            currentView = view.get();
            newView = new View(columnIndex, keyValidator, currentView.getIndexes(), oldSSTables, newIndexes);
        }
        while (!view.compareAndSet(currentView, newView));
    }

    public void dropData(long truncateUntil)
    {
        View currentView = view.get();
        if (currentView == null)
            return;

        Set<SSTableReader> toRemove = new HashSet<>();
        for (SSTableIndex index : currentView)
        {
            if (index.getSSTable().getMaxTimestamp() > truncateUntil)
                continue;

            index.markObsolete();
            toRemove.add(index.getSSTable());
        }

        update(toRemove, Collections.<SSTableReader>emptyList());
    }

    private Set<SSTableIndex> getIndexes(Collection<SSTableReader> sstables)
    {
        Set<SSTableIndex> indexes = new HashSet<>(sstables.size());
        for (SSTableReader sstable : sstables)
        {
            if (sstable.isMarkedCompacted())
                continue;

            File indexFile = new File(sstable.descriptor.filenameFor(columnIndex.getComponent().name));
            if (!indexFile.exists())
                continue;

            SSTableIndex index = null;

            try
            {
                index = new SSTableIndex(columnIndex, indexFile, sstable);

                logger.info("SSTableIndex.open(column: {}, minTerm: {}, maxTerm: {}, minKey: {}, maxKey: {}, sstable: {})",
                            columnIndex.getColumnName(),
                            columnIndex.getValidator().getString(index.minTerm()),
                            columnIndex.getValidator().getString(index.maxTerm()),
                            keyValidator.getString(index.minKey()),
                            keyValidator.getString(index.maxKey()),
                            index.getSSTable());

                // Try to add new index to the set, if set already has such index, we'll simply release and move on.
                // This covers situation when sstable collection has the same sstable multiple
                // times because we don't know what kind of collection it actually is.
                if (!indexes.add(index))
                    index.release();
            }
            catch (Throwable t)
            {
                logger.error("Can't open index file at " + indexFile.getAbsolutePath() + ", skipping.", t);
                if (index != null)
                    index.release();
            }
        }

        return indexes;
    }
}
