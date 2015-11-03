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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.db.index.sasi.analyzer.NoOpAnalyzer;
import org.apache.cassandra.db.index.sasi.analyzer.NonTokenizingAnalyzer;
import org.apache.cassandra.db.index.sasi.analyzer.StandardAnalyzer;
import org.apache.cassandra.db.index.sasi.disk.OnDiskIndexBuilder.Mode;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMode
{
    private static final Logger logger = LoggerFactory.getLogger(IndexMode.class);

    public static final IndexMode NOT_INDEXED = new IndexMode(Mode.ORIGINAL, true, false, NonTokenizingAnalyzer.class, 0);

    private static final Set<AbstractType<?>> TOKENIZABLE_TYPES = new HashSet<AbstractType<?>>()
    {{
        add(UTF8Type.instance);
        add(AsciiType.instance);
    }};

    private static final String INDEX_MODE_OPTION = "mode";
    private static final String INDEX_ANALYZED_OPTION = "analyzed";
    private static final String INDEX_ANALYZER_CLASS_OPTION = "analyzer_class";
    private static final String INDEX_IS_LITERAL_OPTION = "is_literal";
    private static final String INDEX_MAX_FLUSH_MEMORY_OPTION = "max_compaction_flush_memory_in_mb";
    private static final double INDEX_MAX_FLUSH_DEFAULT_MULTIPLIER = 0.15;

    public final Mode mode;
    public final boolean isAnalyzed, isLiteral;
    public final Class analyzerClass;
    public final long maxCompactionFlushMemoryInMb;

    private IndexMode(Mode mode, boolean isLiteral, boolean isAnalyzed, Class analyzerClass, long maxFlushMemMb)
    {
        this.mode = mode;
        this.isLiteral = isLiteral;
        this.isAnalyzed = isAnalyzed;
        this.analyzerClass = analyzerClass;
        this.maxCompactionFlushMemoryInMb = maxFlushMemMb;
    }

    public void validate(Map<String, String> indexOptions) throws ConfigurationException
    {
        // validate that a valid analyzer class was provided if specified
        if (indexOptions.containsKey(INDEX_ANALYZER_CLASS_OPTION))
        {
            try
            {
                Class.forName(indexOptions.get(INDEX_ANALYZER_CLASS_OPTION));
            }
            catch (ClassNotFoundException e)
            {
                throw new ConfigurationException(String.format("Invalid analyzer class option specified [%s]",
                        indexOptions.get(INDEX_ANALYZER_CLASS_OPTION)));
            }
        }
    }

    public AbstractAnalyzer getAnalyzer(AbstractType<?> validator)
    {
        AbstractAnalyzer analyzer = new NoOpAnalyzer();

        try
        {
            if (isAnalyzed)
            {
                if (analyzerClass != null)
                    analyzer = (AbstractAnalyzer) analyzerClass.newInstance();
                else if (TOKENIZABLE_TYPES.contains(validator))
                    analyzer = new StandardAnalyzer();
            }
        }
        catch (InstantiationException | IllegalAccessException e)
        {
            logger.error("Failed to create new instance of analyzer with class [{}]", analyzerClass.getName(), e);
        }

        return analyzer;
    }

    public static IndexMode getMode(ColumnDefinition column)
    {
        Map<String, String> indexOptions = column.getIndexOptions();
        if (indexOptions == null || indexOptions.isEmpty())
            return IndexMode.NOT_INDEXED;

        Mode mode = indexOptions.get(INDEX_MODE_OPTION) == null
                        ? Mode.ORIGINAL
                        : Mode.mode(indexOptions.get(INDEX_MODE_OPTION));

        boolean isAnalyzed = false;
        Class analyzerClass = null;
        try
        {
            if (indexOptions.get(INDEX_ANALYZER_CLASS_OPTION) != null)
            {
                analyzerClass = Class.forName(indexOptions.get(INDEX_ANALYZER_CLASS_OPTION));
                isAnalyzed = indexOptions.get(INDEX_ANALYZED_OPTION) == null
                              ? true : Boolean.valueOf(indexOptions.get(INDEX_ANALYZED_OPTION));
            }
            else if (indexOptions.get(INDEX_ANALYZED_OPTION) != null)
            {
                isAnalyzed = Boolean.valueOf(indexOptions.get(INDEX_ANALYZED_OPTION));
            }
        }
        catch (ClassNotFoundException e)
        {
            // should not happen as we already validated we could instantiate an instance in validateOptions()
            logger.error("Failed to find specified analyzer class [{}]. Falling back to default analyzer",
                         indexOptions.get(INDEX_ANALYZER_CLASS_OPTION));
        }

        boolean isLiteral = false;
        try
        {
            String literalOption = indexOptions.get(INDEX_IS_LITERAL_OPTION);
            AbstractType<?> validator = column.getValidator();

            isLiteral = literalOption == null
                            ? (validator instanceof UTF8Type || validator instanceof AsciiType)
                            : Boolean.valueOf(literalOption);
        }
        catch (Exception e)
        {
            logger.error("failed to parse {} option, defaulting to 'false' for {} index.", INDEX_IS_LITERAL_OPTION, column.getIndexName());
        }

        Long maxMemMb = indexOptions.get(INDEX_MAX_FLUSH_MEMORY_OPTION) == null
                ? (long) ((DatabaseDescriptor.getTotalMemtableSpaceInMB() * 1048576L) * INDEX_MAX_FLUSH_DEFAULT_MULTIPLIER)
                : Long.parseLong(indexOptions.get(INDEX_MAX_FLUSH_MEMORY_OPTION));

        return new IndexMode(mode, isLiteral, isAnalyzed, analyzerClass, maxMemMb);
    }
}
