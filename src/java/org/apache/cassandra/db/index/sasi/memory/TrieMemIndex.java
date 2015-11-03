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
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.db.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.db.index.sasi.disk.Token;
import org.apache.cassandra.db.index.sasi.plan.Expression;
import org.apache.cassandra.db.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.db.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.db.index.sasi.utils.RangeIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.StorageService;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.suffix.ConcurrentSuffixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.SmartArrayBasedNodeFactory;

import org.github.jamm.MemoryMeter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrieMemIndex extends MemIndex
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemIndex.class);

    private final ConcurrentTrie index;

    public TrieMemIndex(AbstractType<?> keyValidator, ColumnIndex columnIndex)
    {
        super(keyValidator, columnIndex);

        switch (columnIndex.getMode().mode)
        {
            case SUFFIX:
                index = new ConcurrentSuffixTrie(columnIndex.getDefinition());
                break;

            case ORIGINAL:
                index = new ConcurrentPrefixTrie(columnIndex.getDefinition());
                break;

            default:
                throw new IllegalStateException("Unsupported mode: " + columnIndex.getMode().mode);
        }
    }

    @Override
    public void add(ByteBuffer value, ByteBuffer key)
    {
        final DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);

        AbstractAnalyzer analyzer = columnIndex.getAnalyzer();
        analyzer.reset(value.duplicate());

        while (analyzer.hasNext())
        {
            ByteBuffer term = analyzer.next();

            if (term.remaining() >= OnDiskIndexBuilder.MAX_TERM_SIZE)
            {
                logger.info("Can't add term of column {} to index for key: {}, term size {} bytes, max allowed size {} bytes, use analyzed = true (if not yet set) for that column.",
                            columnIndex.getColumnName(),
                            keyValidator.getString(key),
                            term.remaining(),
                            OnDiskIndexBuilder.MAX_TERM_SIZE);
                continue;
            }

            index.add(columnIndex.getValidator().getString(term), dk);
        }
    }

    @Override
    public RangeIterator<Long, Token> search(Expression expression)
    {
        return index.search(expression);
    }

    @Override
    public long estimateSize(MemoryMeter meter)
    {
        return meter.measureDeep(index);
    }

    private static abstract class ConcurrentTrie
    {
        protected final ColumnDefinition definition;

        public ConcurrentTrie(ColumnDefinition column)
        {
            definition = column;
        }

        public void add(String value, DecoratedKey key)
        {
            ConcurrentSkipListSet<DecoratedKey> keys = get(value);
            if (keys == null)
            {
                ConcurrentSkipListSet<DecoratedKey> newKeys = new ConcurrentSkipListSet<>(DecoratedKey.comparator);
                keys = putIfAbsent(value, newKeys);
                if (keys == null)
                    keys = newKeys;
            }

            keys.add(key);
        }

        public RangeIterator<Long, Token> search(Expression expression)
        {
            assert expression.getOp() == Expression.Op.EQ; // means that min == max

            ByteBuffer prefix = expression.lower == null ? null : expression.lower.value;

            Iterable<ConcurrentSkipListSet<DecoratedKey>> search = search(definition.getValidator().getString(prefix));

            RangeUnionIterator.Builder<Long, Token> builder = RangeUnionIterator.builder();
            for (ConcurrentSkipListSet<DecoratedKey> keys : search)
            {
                if (!keys.isEmpty())
                    builder.add(new KeyRangeIterator(keys));
            }

            return builder.build();
        }

        protected abstract ConcurrentSkipListSet<DecoratedKey> get(String value);
        protected abstract Iterable<ConcurrentSkipListSet<DecoratedKey>> search(String value);
        protected abstract ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> key);
    }

    private static class ConcurrentPrefixTrie extends ConcurrentTrie
    {
        private final ConcurrentRadixTree<ConcurrentSkipListSet<DecoratedKey>> trie;

        private ConcurrentPrefixTrie(ColumnDefinition column)
        {
            super(column);
            trie = new ConcurrentRadixTree<>(new SmartArrayBasedNodeFactory());
        }

        @Override
        public ConcurrentSkipListSet<DecoratedKey> get(String value)
        {
            return trie.getValueForExactKey(value);
        }

        @Override
        public ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> newKeys)
        {
            return trie.putIfAbsent(value, newKeys);
        }

        @Override
        public Iterable<ConcurrentSkipListSet<DecoratedKey>> search(String value)
        {
            return trie.getValuesForKeysStartingWith(value);
        }
    }

    private static class ConcurrentSuffixTrie extends ConcurrentTrie
    {
        private final ConcurrentSuffixTree<ConcurrentSkipListSet<DecoratedKey>> trie;

        private ConcurrentSuffixTrie(ColumnDefinition column)
        {
            super(column);
            trie = new ConcurrentSuffixTree<>(new SmartArrayBasedNodeFactory());
        }

        @Override
        public ConcurrentSkipListSet<DecoratedKey> get(String value)
        {
            return trie.getValueForExactKey(value);
        }

        @Override
        public ConcurrentSkipListSet<DecoratedKey> putIfAbsent(String value, ConcurrentSkipListSet<DecoratedKey> newKeys)
        {
            return trie.putIfAbsent(value, newKeys);
        }

        @Override
        public Iterable<ConcurrentSkipListSet<DecoratedKey>> search(String value)
        {
            return trie.getValuesForKeysContaining(value);
        }
    }
}
