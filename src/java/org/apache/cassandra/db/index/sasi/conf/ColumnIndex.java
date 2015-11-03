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

import java.util.*;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.db.index.sasi.conf.view.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;

public class ColumnIndex
{
    private static final String FILE_NAME_FORMAT = "SI_%s.db";

    private final ColumnDefinition column;
    private final AbstractType<?> comparator;
    private final IndexMode mode;

    private final Component component;
    private final DataTracker tracker;

    public ColumnIndex(AbstractType<?> keyValidator, ColumnDefinition column, AbstractType<?> comparator)
    {
        this.column = column;
        this.comparator = comparator;
        this.mode = IndexMode.getMode(column);
        this.tracker = new DataTracker(keyValidator, this, Collections.<SSTableReader>emptySet());
        this.component = new Component(Component.Type.SECONDARY_INDEX, String.format(FILE_NAME_FORMAT, column.getIndexName()));
    }

    public void validate() throws ConfigurationException
    {
        mode.validate(column.getIndexOptions());
    }

    public void update(Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newSSTables)
    {
        tracker.update(oldSSTables, newSSTables);
    }

    public ColumnDefinition getDefinition()
    {
        return column;
    }

    public AbstractType<?> getValidator()
    {
        return column.getValidator();
    }

    public Component getComponent()
    {
        return component;
    }

    public IndexMode getMode()
    {
        return mode;
    }

    public String getColumnName()
    {
        return comparator.getString(column.name);
    }

    public String getIndexName()
    {
        return column.getIndexName();
    }

    public AbstractAnalyzer getAnalyzer()
    {
        AbstractAnalyzer analyzer = mode.getAnalyzer(getValidator());
        analyzer.init(column.getIndexOptions(), column.getValidator());
        return analyzer;
    }

    public View getView()
    {
        return tracker.getView();
    }

    public void dropData(long truncateUntil)
    {
        tracker.dropData(truncateUntil);
    }

    public boolean isIndexed()
    {
        return mode != IndexMode.NOT_INDEXED;
    }

    public boolean isLiteral()
    {
        AbstractType<?> validator = getValidator();
        return isIndexed() ? mode.isLiteral : (validator instanceof UTF8Type || validator instanceof AsciiType);
    }

    public String getPath(Descriptor sstable)
    {
        return sstable.filenameFor(component);
    }
}
