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

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.index.sasi.disk.Token;
import org.apache.cassandra.db.index.sasi.utils.AbstractIterator;
import org.apache.cassandra.db.index.sasi.utils.CombinedValue;
import org.apache.cassandra.db.index.sasi.utils.RangeIterator;

import com.google.common.collect.PeekingIterator;

public class KeyRangeIterator extends RangeIterator<Long, Token>
{
    private final DKIterator iterator;

    public KeyRangeIterator(ConcurrentSkipListSet<DecoratedKey> keys)
    {
        super((Long) keys.first().getToken().token, (Long) keys.last().getToken().token, keys.size());
        this.iterator = new DKIterator(keys.iterator());
    }

    @Override
    protected Token computeNext()
    {
        return iterator.hasNext() ? new DKToken(iterator.next()) : endOfData();
    }

    @Override
    protected void performSkipTo(Long nextToken)
    {
        while (iterator.hasNext())
        {
            DecoratedKey key = iterator.peek();
            if (Long.compare((long) key.token.token, nextToken) >= 0)
                break;

            // consume smaller key
            iterator.next();
        }
    }

    @Override
    public void close() throws IOException
    {}

    private static class DKIterator extends AbstractIterator<DecoratedKey> implements PeekingIterator<DecoratedKey>
    {
        private final Iterator<DecoratedKey> keys;

        public DKIterator(Iterator<DecoratedKey> keys)
        {
            this.keys = keys;
        }

        @Override
        protected DecoratedKey computeNext()
        {
            return keys.hasNext() ? keys.next() : endOfData();
        }
    }

    private static class DKToken extends Token
    {
        private final SortedSet<DecoratedKey> keys;

        public DKToken(final DecoratedKey key)
        {
            super((long) key.token.token);

            keys = new TreeSet<DecoratedKey>(DecoratedKey.comparator)
            {{
                add(key);
            }};
        }

        @Override
        public void merge(CombinedValue<Long> other)
        {
            if (!(other instanceof Token))
                return;

            Token o = (Token) other;
            assert o.get().equals(token);

            if (o instanceof DKToken)
            {
                keys.addAll(((DKToken) o).keys);
            }
            else
            {
                for (DecoratedKey key : o)
                    keys.add(key);
            }
        }

        @Override
        public Iterator<DecoratedKey> iterator()
        {
            return keys.iterator();
        }
    }
}