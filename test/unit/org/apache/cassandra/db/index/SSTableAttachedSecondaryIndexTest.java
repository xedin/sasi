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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.db.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.db.index.sasi.exceptions.TimeQuotaExceededException;
import org.apache.cassandra.db.index.sasi.plan.QueryPlan;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;

import junit.framework.Assert;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class SSTableAttachedSecondaryIndexTest extends SchemaLoader
{
    private static final String KS_NAME = "sasecondaryindex";
    private static final String CF_NAME = "saindexed1";

    @BeforeClass
    public static void loadSchema() throws IOException, ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        loadSchema(false);
    }
    
    @After
    public void cleanUp()
    {
        Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).truncateBlocking();
    }

    @Test
    public void testSingleExpressionQueries() throws Exception
    {
        testSingleExpressionQueries(false);
        cleanupData();
        testSingleExpressionQueries(true);
    }

    private void testSingleExpressionQueries(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
            put("key1", Pair.create("Pavel", 14));
            put("key2", Pair.create("Pavel", 26));
            put("key3", Pair.create("Pavel", 27));
            put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("av")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("as")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("aw")));
        Assert.assertEquals(rows.toString(), 0, rows.size());

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(27)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{"key3", "key4"}, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(26)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(13)));
        Assert.assertEquals(rows.toString(), 0, rows.size());
    }

    @Test
    public void testEmptyTokenizedResults() throws Exception
    {
        testEmptyTokenizedResults(false);
        cleanupData();
        testEmptyTokenizedResults(true);
    }

    private void testEmptyTokenizedResults(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("  ", 14));
        }};

        ColumnFamilyStore store = loadData(data, forceFlush);

        Set<String> rows= getIndexed(store, 10, new IndexExpression(UTF8Type.instance.decompose("first_name"), IndexOperator.EQ, UTF8Type.instance.decompose("doesntmatter")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{}, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testMultiExpressionQueries() throws Exception
    {
        testMultiExpressionQueries(false);
        cleanupData();
        testMultiExpressionQueries(true);
    }

    public void testMultiExpressionQueries(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> data = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                                      new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                      new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(14)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(27)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{"key1", "key2"}, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(14)),
                         new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(27)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(12)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(13)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(16)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));


        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(30)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(29)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                         new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                         new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(25)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testCrossSSTableQueries() throws Exception
    {
        testCrossSSTableQueries(false);
        cleanupData();
        testCrossSSTableQueries(true);

    }

    private void testCrossSSTableQueries(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", 43));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create("Josephine", 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
            }};

        loadData(part1, forceFlush); // first sstable

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Charley", 21));
                put("key9", Pair.create("Amely", 40));
            }};

        loadData(part2, forceFlush);

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key10", Pair.create("Eddie", 42));
                put("key11", Pair.create("Oswaldo", 35));
                put("key12", Pair.create("Susana", 35));
                put("key13", Pair.create("Alivia", 42));
                put("key14", Pair.create("Demario", 28));
            }};

        ColumnFamilyStore store = loadData(part3, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                                      new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Fiona")),
                                      new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(40)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key6" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key14",
                                                                        "key3", "key4", "key6", "key7", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 5,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        Assert.assertEquals(rows.toString(), 5, rows.size());

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(35)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key4", "key6", "key7" },
                                                         rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14", "key3", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(27)),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(10)));

        Assert.assertEquals(rows.toString(), 10, rows.size());

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(50)));

        Assert.assertEquals(rows.toString(), 10, rows.size());
    }

    @Test
    public void testQueriesThatShouldBeTokenized() throws Exception
    {
        testQueriesThatShouldBeTokenized(false);
        cleanupData();
        testQueriesThatShouldBeTokenized(true);
    }

    private void testQueriesThatShouldBeTokenized(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("If you can dream it, you can do it.", 43));
                put("key1", Pair.create("What you get by achieving your goals is not " +
                        "as important as what you become by achieving your goals, do it.", 33));
                put("key2", Pair.create("Keep your face always toward the sunshine " +
                        "- and shadows will fall behind you.", 43));
                put("key3", Pair.create("We can't help everyone, but everyone can " +
                        "help someone.", 27));
            }};

        ColumnFamilyStore store = loadData(part1, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                new IndexExpression(firstName, IndexOperator.EQ,
                        UTF8Type.instance.decompose("What you get by achieving your goals")),
                new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(32)));

        Assert.assertEquals(rows.toString(), Collections.singleton("key1"), rows);

        rows = getIndexed(store, 10,
                new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("do it.")));

        Assert.assertEquals(rows.toString(), Arrays.asList("key0", "key1"), Lists.newArrayList(rows));
    }

    @Test
    public void testMultiExpressionQueriesWhereRowSplitBetweenSSTables() throws Exception
    {
        testMultiExpressionQueriesWhereRowSplitBetweenSSTables(false);
        cleanupData();
        testMultiExpressionQueriesWhereRowSplitBetweenSSTables(true);
    }

    private void testMultiExpressionQueriesWhereRowSplitBetweenSSTables(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", -1));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create((String)null, 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
        }};

        loadData(part1, forceFlush); // first sstable

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Charley", 21));
                put("key9", Pair.create("Amely", 40));
                put("key14", Pair.create((String)null, 28));
        }};

        loadData(part2, forceFlush);

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create((String)null, 43));
                put("key10", Pair.create("Eddie", 42));
                put("key11", Pair.create("Oswaldo", 35));
                put("key12", Pair.create("Susana", 35));
                put("key13", Pair.create("Alivia", 42));
                put("key14", Pair.create("Demario", -1));
                put("key2", Pair.create("Josephine", -1));
        }};

        ColumnFamilyStore store = loadData(part3, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10,
                                      new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Fiona")),
                                      new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(40)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key6" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key14",
                                                                        "key3", "key4", "key6", "key7", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 5,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        Assert.assertEquals(rows.toString(), 5, rows.size());

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(35)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key0", "key11", "key12", "key13", "key4", "key6", "key7" },
                                                         rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14", "key3", "key8" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(27)),
                          new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(32)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14" }, rows.toArray(new String[rows.size()])));

        Map<String, Pair<String, Integer>> part4 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key12", Pair.create((String)null, 12));
                put("key14", Pair.create("Demario", 42));
                put("key2", Pair.create("Frank", -1));
        }};

        store = loadData(part4, forceFlush);

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Susana")),
                          new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(13)),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(10)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key12" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Demario")),
                          new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(30)));
        Assert.assertTrue(rows.toString(), rows.size() == 0);

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("Josephine")));
        Assert.assertTrue(rows.toString(), rows.size() == 0);

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(10)));

        Assert.assertEquals(rows.toString(), 10, rows.size());

        rows = getIndexed(store, 10,
                          new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                          new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(50)));

        Assert.assertEquals(rows.toString(), 10, rows.size());
    }

    @Test
    public void testPagination() throws Exception
    {
        testPagination(false);
        cleanupData();
        testPagination(true);
    }

    private void testPagination(boolean forceFlush) throws Exception
    {
        // split data into 3 distinct SSTables to test paging with overlapping token intervals.

        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key01", Pair.create("Ali", 33));
                put("key02", Pair.create("Jeremy", 41));
                put("key03", Pair.create("Elvera", 22));
                put("key04", Pair.create("Bailey", 45));
                put("key05", Pair.create("Emerson", 32));
                put("key06", Pair.create("Kadin", 38));
                put("key07", Pair.create("Maggie", 36));
                put("key08", Pair.create("Kailey", 36));
                put("key09", Pair.create("Armand", 21));
                put("key10", Pair.create("Arnold", 35));
        }};

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key11", Pair.create("Ken", 38));
                put("key12", Pair.create("Penelope", 43));
                put("key13", Pair.create("Wyatt", 34));
                put("key14", Pair.create("Johnpaul", 34));
                put("key15", Pair.create("Trycia", 43));
                put("key16", Pair.create("Aida", 21));
                put("key17", Pair.create("Devon", 42));
        }};

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key18", Pair.create("Christina", 20));
                put("key19", Pair.create("Rick", 19));
                put("key20", Pair.create("Fannie", 22));
                put("key21", Pair.create("Keegan", 29));
                put("key22", Pair.create("Ignatius", 36));
                put("key23", Pair.create("Ellis", 26));
                put("key24", Pair.create("Annamarie", 29));
                put("key25", Pair.create("Tianna", 31));
                put("key26", Pair.create("Dennis", 32));
        }};

        ColumnFamilyStore store = loadData(part1, forceFlush);

        loadData(part2, forceFlush);
        loadData(part3, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<DecoratedKey> uniqueKeys = getPaged(store, 4,
                new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(21)));


        List<String> expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key04");
                add("key08");
                add("key07");
                add("key15");
                add("key06");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // now let's test a single equals condition

        uniqueKeys = getPaged(store, 4, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));

        expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key04");
                add("key18");
                add("key08");
                add("key07");
                add("key15");
                add("key06");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // now let's test something which is smaller than a single page
        uniqueKeys = getPaged(store, 4,
                              new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                              new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(36)));

        expected = new ArrayList<String>()
        {{
                add("key22");
                add("key08");
                add("key07");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        // the same but with the page size of 2 to test minimal pagination windows

        uniqueKeys = getPaged(store, 2,
                              new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                              new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(36)));

        Assert.assertEquals(expected, convert(uniqueKeys));

        // and last but not least, test age range query with pagination
        uniqueKeys = getPaged(store, 4,
                new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(20)),
                new IndexExpression(age, IndexOperator.LTE, Int32Type.instance.decompose(36)));

        expected = new ArrayList<String>()
        {{
                add("key25");
                add("key20");
                add("key13");
                add("key22");
                add("key09");
                add("key14");
                add("key16");
                add("key24");
                add("key03");
                add("key08");
                add("key07");
                add("key21");
        }};

        Assert.assertEquals(expected, convert(uniqueKeys));

        Set<String> rows;

        rows = executeCQL(String.format("SELECT * FROM %s.%s WHERE first_name = 'a' limit 10 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key03", "key04", "key09", "key13", "key14", "key16", "key20", "key22", "key24", "key25" }, rows.toArray(new String[rows.size()])));

        rows = executeCQL(String.format("SELECT * FROM %s.%s WHERE first_name = 'a' and token(key) >= token('key14') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key03", "key04", "key14", "key16", "key24" }, rows.toArray(new String[rows.size()])));

        rows = executeCQL(String.format("SELECT * FROM %s.%s WHERE first_name = 'a' and token(key) >= token('key14') and token(key) <= token('key24') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14", "key16", "key24" }, rows.toArray(new String[rows.size()])));

        rows = executeCQL(String.format("SELECT * FROM %s.%s WHERE first_name = 'a' and age > 30 and token(key) >= token('key14') and token(key) <= token('key24') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14" }, rows.toArray(new String[rows.size()])));

        rows = executeCQL(String.format("SELECT * FROM %s.%s WHERE first_name = 'a' or age > 30 and token(key) >= token('key14') and token(key) <= token('key24') limit 5 ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key14", "key16", "key24" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testColumnNamesWithSlashes()
    {
        testColumnNamesWithSlashes(false);
        cleanupData();
        testColumnNamesWithSlashes(true);
    }

    private void testColumnNamesWithSlashes(boolean forceFlush)
    {
        RowMutation rm1 = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm1.add(CF_NAME, UTF8Type.instance.decompose("/data/output/id"), AsciiType.instance.decompose("jason"), System.currentTimeMillis());

        RowMutation rm2 = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm2.add(CF_NAME, UTF8Type.instance.decompose("/data/output/id"), AsciiType.instance.decompose("pavel"), System.currentTimeMillis());

        RowMutation rm3 = new RowMutation(KS_NAME, AsciiType.instance.decompose("key3"));
        rm3.add(CF_NAME, UTF8Type.instance.decompose("/data/output/id"), AsciiType.instance.decompose("Aleksey"), System.currentTimeMillis());

        rm1.apply();
        rm2.apply();
        rm3.apply();

        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        if (forceFlush)
            store.forceBlockingFlush();

        final ByteBuffer dataOutputId = UTF8Type.instance.decompose("/data/output/id");

        Set<String> rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("A")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key3" }, rows.toArray(new String[rows.size()])));

        // we are going to delete this column and right after we validate results we'll bring it back to life
        ColumnDefinition zombie = store.metadata.getColumnDefinition(dataOutputId);

        store.indexManager.removeIndexedColumn(dataOutputId);

        rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("A")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        store.indexManager.addIndexedColumn(zombie);

        // doesn't really make sense to rebuild index for in-memory data
        if (!forceFlush)
            return;

        store.indexManager.invalidate();

        rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("A")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        // now let's trigger index rebuild and check if we got the data back
        store.indexManager.maybeBuildSecondaryIndexes(store.getSSTables(), Collections.singleton("data_output_id"));

        rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2" }, rows.toArray(new String[rows.size()])));

        // also let's try to build an index for column which has no data to make sure that doesn't fail
        store.indexManager.maybeBuildSecondaryIndexes(store.getSSTables(), Collections.singleton("first_name,data_output_id"));

        rows = getIndexed(store, 10, new IndexExpression(dataOutputId, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testInvalidate() throws Exception
    {
        testInvalidate(false);
        cleanupData();
        testInvalidate(true);
    }

    private void testInvalidate(boolean forceFlush) throws Exception
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key0", Pair.create("Maxie", -1));
                put("key1", Pair.create("Chelsie", 33));
                put("key2", Pair.create((String) null, 43));
                put("key3", Pair.create("Shanna", 27));
                put("key4", Pair.create("Amiya", 36));
        }};

        ColumnFamilyStore store = loadData(part1, forceFlush);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Set<String> rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key0", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(33)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1" }, rows.toArray(new String[rows.size()])));

        store.indexManager.invalidate();

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), rows.isEmpty());

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(33)));
        Assert.assertTrue(rows.toString(), rows.isEmpty());


        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key5", Pair.create("Americo", 20));
                put("key6", Pair.create("Fiona", 39));
                put("key7", Pair.create("Francis", 41));
                put("key8", Pair.create("Fred", 21));
                put("key9", Pair.create("Amely", 40));
                put("key14", Pair.create("Dino", 28));
        }};

        loadData(part2, forceFlush);

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key6", "key7" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(40)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key9" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testTruncate()
    {
        Map<String, Pair<String, Integer>> part1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key01", Pair.create("Ali", 33));
                put("key02", Pair.create("Jeremy", 41));
                put("key03", Pair.create("Elvera", 22));
                put("key04", Pair.create("Bailey", 45));
                put("key05", Pair.create("Emerson", 32));
                put("key06", Pair.create("Kadin", 38));
                put("key07", Pair.create("Maggie", 36));
                put("key08", Pair.create("Kailey", 36));
                put("key09", Pair.create("Armand", 21));
                put("key10", Pair.create("Arnold", 35));
        }};

        Map<String, Pair<String, Integer>> part2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key11", Pair.create("Ken", 38));
                put("key12", Pair.create("Penelope", 43));
                put("key13", Pair.create("Wyatt", 34));
                put("key14", Pair.create("Johnpaul", 34));
                put("key15", Pair.create("Trycia", 43));
                put("key16", Pair.create("Aida", 21));
                put("key17", Pair.create("Devon", 42));
        }};

        Map<String, Pair<String, Integer>> part3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key18", Pair.create("Christina", 20));
                put("key19", Pair.create("Rick", 19));
                put("key20", Pair.create("Fannie", 22));
                put("key21", Pair.create("Keegan", 29));
                put("key22", Pair.create("Ignatius", 36));
                put("key23", Pair.create("Ellis", 26));
                put("key24", Pair.create("Annamarie", 29));
                put("key25", Pair.create("Tianna", 31));
                put("key26", Pair.create("Dennis", 32));
        }};

        ColumnFamilyStore store = loadData(part1, 1000, true);

        loadData(part2, 2000, true);
        loadData(part3, 3000, true);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");

        Set<String> rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 16, rows.size());

        // make sure we don't prematurely delete anything
        for (SecondaryIndex index : store.indexManager.getIndexes())
            index.truncateBlocking(500);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 16, rows.size());

        for (SecondaryIndex index : store.indexManager.getIndexes())
            index.truncateBlocking(1500);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 10, rows.size());

        for (SecondaryIndex index : store.indexManager.getIndexes())
            index.truncateBlocking(2500);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 6, rows.size());

        for (SecondaryIndex index : store.indexManager.getIndexes())
            index.truncateBlocking(3500);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 0, rows.size());

        // add back in some data just to make sure it all still works
        Map<String, Pair<String, Integer>> part4 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key40", Pair.create("Tianna", 31));
                put("key41", Pair.create("Dennis", 32));
        }};

        loadData(part4, 4000, true);

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertEquals(rows.toString(), 1, rows.size());
    }

    @Test
    public void testSearchWithoutOrPartialPredicateFiltering()
    {
        testSearchWithoutOrPartialPredicateFiltering(false);
        cleanupData();
        testSearchWithoutOrPartialPredicateFiltering(true);
    }

    private void testSearchWithoutOrPartialPredicateFiltering(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        RowMutation rm1 = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm1.add(CF_NAME, firstName, AsciiType.instance.decompose("pavel"), System.currentTimeMillis());
        rm1.add(CF_NAME, age, Int32Type.instance.decompose(26), System.currentTimeMillis());
        rm1.add(CF_NAME, UTF8Type.instance.decompose("/data/1"), Int32Type.instance.decompose(1), System.currentTimeMillis());
        rm1.add(CF_NAME, UTF8Type.instance.decompose("/data/2"), Int32Type.instance.decompose(2), System.currentTimeMillis());
        rm1.add(CF_NAME, UTF8Type.instance.decompose("/data/3"), Int32Type.instance.decompose(3), System.currentTimeMillis());
        rm1.add(CF_NAME, UTF8Type.instance.decompose("/data/4"), Int32Type.instance.decompose(4), System.currentTimeMillis());

        rm1.apply();

        if (forceFlush)
            store.forceBlockingFlush();

        // don't request any columns that are in the index expressions
        SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(store.getComparator())
        {{
            add(UTF8Type.instance.decompose("/data/2"));
        }};

        Set<String> rows = getIndexed(store, new NamesQueryFilter(columns), 10,
                                      new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                      new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(26)));

        Assert.assertEquals(rows.toString(), 1, rows.size());
        Assert.assertEquals(rows.toString(), "key1", Iterables.get(rows, 0));

        // now, let's request only one of the expressions to be returned as a column, this will make sure
        // that when missing columns are filtered, only appropriate expressions are taken into consideration.
        columns = new TreeSet<ByteBuffer>(store.getComparator())
        {{
                add(UTF8Type.instance.decompose("/data/1"));
                add(UTF8Type.instance.decompose("/data/2"));
                add(firstName);
        }};

        getIndexed(store, new NamesQueryFilter(columns), 10,
                   new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                   new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(26)));

        Assert.assertEquals(rows.toString(), 1, rows.size());
        Assert.assertEquals(rows.toString(), "key1", Iterables.get(rows, 0));
    }

    @Test
    public void testORandParenthesisExpressions() throws Exception
    {
        testORandParenthesisExpressions(false);
        cleanupData();
        testORandParenthesisExpressions(true);
    }

    private void testORandParenthesisExpressions(boolean forceFlush) throws Exception
    {
        final ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        newMutation("key1", "pavel",   "y", 14, System.currentTimeMillis()).apply();
        newMutation("key2", "pavel",   "y", 26, System.currentTimeMillis()).apply();
        newMutation("key3", "pavel",   "y", 27, System.currentTimeMillis()).apply();

        if (forceFlush)
            store.forceBlockingFlush();

        newMutation("key4", "jason",   "b", 27, System.currentTimeMillis()).apply();
        newMutation("key5", "aleksey", "y", 28, System.currentTimeMillis()).apply();

        if (forceFlush)
            store.forceBlockingFlush();

        IndexExpression[] expressions = getExpressions("SELECT * FROM %s.%s WHERE age = 14 OR age = 27 ALLOW FILTERING;");
        Set<String> rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE last_name = 'y' AND first_name = 'p' OR age = 28 ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key5" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE last_name = 'b' OR (last_name = 'y' AND age >= 14 AND age < 28) ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'j' OR age = 26 OR age = 28 ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key4", "key5" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'j' OR (age > 26 AND age <= 28) ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3", "key4", "key5" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'j' OR age = 26 ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key4" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'j' OR (first_name = 'p' AND age = 26) ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key4" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'j' OR (age > 13 AND age <= 26 AND first_name = 'p') ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key4" }, rows.toArray(new String[rows.size()])));

        // see if merging works cross SSTables works properly with OR expressions
        newMutation("key2", "oksana", "y", 26, System.currentTimeMillis()).apply();

        if (forceFlush)
            store.forceBlockingFlush();

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'j' OR ((age > 13 AND age <= 26) AND first_name = 'p') ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key4" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'j' OR (age > 13 AND age <= 26) AND first_name = 'p' ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'j' OR (age > 13 AND (age <= 26 AND first_name = 'p')) ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key4" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE (first_name = 'j' OR (age > 13 AND (age <= 26 AND first_name = 'p'))) ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key4" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testConcurrentMemtableReadsAndWrites() throws Exception
    {
        final ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        ExecutorService scheduler = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        final int writeCount = 10000;
        final AtomicInteger updates = new AtomicInteger(0);

        for (int i = 0; i < writeCount; i++)
        {
            final String key = "key" + i;
            final String firstName = "first_name#" + i;
            final String lastName = "last_name#" + i;

            scheduler.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        newMutation(key, firstName, lastName, 26, System.currentTimeMillis()).apply();
                        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS); // back up a bit to do more reads
                    }
                    finally
                    {
                        updates.incrementAndGet();
                    }
                }
            });
        }

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        int previousCount = 0;

        do
        {
            // this loop figures out if number of search results monotonically increasing
            // to make sure that concurrent updates don't interfere with reads, uses first_name and age
            // indexes to test correctness of both Trie and SkipList ColumnIndex implementations.

            Set<DecoratedKey> rows = getPaged(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                                          new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(26)));

            Assert.assertTrue(previousCount <= rows.size());
            previousCount = rows.size();
        }
        while (updates.get() < writeCount);

        // to make sure that after all of the right are done we can read all "count" worth of rows
        Set<DecoratedKey> rows = getPaged(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                                      new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(26)));

        Assert.assertEquals(writeCount, rows.size());
    }

    @Test
    public void testSameKeyInMemtableAndSSTables()
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Map<String, Pair<String, Integer>> data1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data1, true);

        Map<String, Pair<String, Integer>> data2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 28));
        }};

        loadData(data2, true);

        Map<String, Pair<String, Integer>> data3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 15));
                put("key4", Pair.create("Jason", 29));
        }};

        loadData(data3, false);

        Set<String> rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));


        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                      new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(15)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                      new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(29)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 100, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                      new IndexExpression(age, IndexOperator.EQ, Int32Type.instance.decompose(27)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{"key2", "key3"}, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testInsertingIncorrectValuesIntoAgeIndex()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        RowMutation rm1 = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm1.add(CF_NAME, firstName, AsciiType.instance.decompose("pavel"), System.currentTimeMillis());
        rm1.add(CF_NAME, age, LongType.instance.decompose(26L), System.currentTimeMillis());
        rm1.apply();

        store.forceBlockingFlush();

        Set<String> rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                                 new IndexExpression(age, IndexOperator.GTE, Int32Type.instance.decompose(26)));

        // index is expected to have 0 results because age value was of wrong type
        Assert.assertEquals(0, rows.size());
    }

    @Test
    public void testInsertWithTypeCasts() throws Exception
    {
        testInsertWithTypeCasts(false);
        cleanupData();
        testInsertWithTypeCasts(true);
    }

    public void testInsertWithTypeCasts(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer timestamp = UTF8Type.instance.decompose("timestamp");
        final ByteBuffer score = UTF8Type.instance.decompose("score");

        RowMutation rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm.add(CF_NAME, firstName, AsciiType.instance.decompose("pavel"), System.currentTimeMillis());
        rm.add(CF_NAME, timestamp, Int32Type.instance.decompose(10), System.currentTimeMillis());
        rm.add(CF_NAME, score, DoubleType.instance.decompose(0.1d), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm.add(CF_NAME, firstName, AsciiType.instance.decompose("jason"), System.currentTimeMillis());
        rm.add(CF_NAME, timestamp, (ByteBuffer) ByteBuffer.allocate(2).putShort((short) 9).flip(), System.currentTimeMillis());
        rm.add(CF_NAME, score, FloatType.instance.decompose(0.2f), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key3"));
        rm.add(CF_NAME, firstName, AsciiType.instance.decompose("jordan"), System.currentTimeMillis());
        rm.add(CF_NAME, timestamp, LongType.instance.decompose(11L), System.currentTimeMillis());
        rm.add(CF_NAME, score, FloatType.instance.decompose(0.3f), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key4"));
        rm.add(CF_NAME, firstName, AsciiType.instance.decompose("michael"), System.currentTimeMillis());
        rm.add(CF_NAME, timestamp, UTF8Type.instance.decompose("0"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key5"));
        rm.add(CF_NAME, firstName, AsciiType.instance.decompose("mikhail"), System.currentTimeMillis());
        rm.add(CF_NAME, timestamp, UTF8Type.instance.decompose("200560490131"), System.currentTimeMillis());
        rm.apply();

        if (forceFlush)
            store.forceBlockingFlush();

        Set<String> rows = getIndexed(store, 10, new IndexExpression(timestamp, IndexOperator.EQ, LongType.instance.decompose(9L)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(timestamp, IndexOperator.EQ, LongType.instance.decompose(10L)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(timestamp, IndexOperator.EQ, LongType.instance.decompose(11L)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(timestamp, IndexOperator.GTE, LongType.instance.decompose(9L)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1", "key2", "key3", "key5" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(score, IndexOperator.GTE, DoubleType.instance.decompose(0.1d)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key1", "key2", "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(score, IndexOperator.GTE, DoubleType.instance.decompose(0.2d)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key2", "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(timestamp, IndexOperator.EQ, LongType.instance.decompose(0L)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(timestamp, IndexOperator.EQ, LongType.instance.decompose(200560490131L)));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[]{ "key5" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testNotSupport() throws Exception
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer age = UTF8Type.instance.decompose("age");

        Map<String, Pair<String, Integer>> data1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data1, true);

        Map<String, Pair<String, Integer>> data2 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 28));
        }};

        loadData(data2, true);

        Map<String, Pair<String, Integer>> data3 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 15));
                put("key4", Pair.create("Jason", 29));
        }};

        loadData(data3, false);

        /* Thrift */

        Set<String> rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                                 new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(27)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(10)),
                                     new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(27)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(10)),
                                     new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(27)),
                                     new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(29)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(age, IndexOperator.GT, Int32Type.instance.decompose(10)),
                                     new IndexExpression(age, IndexOperator.NOT_EQ, Int32Type.instance.decompose(29)),
                                     new IndexExpression(age, IndexOperator.LT, Int32Type.instance.decompose(30)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3" }, rows.toArray(new String[rows.size()])));

        /* CQL3 */

        IndexExpression[] expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'a' AND age != 27 ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key4" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'a' AND age != 27 AND age > 10 ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key4" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'a' AND age > 10 AND age != 27 AND age < 29 ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        expressions = getExpressions("SELECT * FROM %s.%s WHERE first_name = 'a' AND age > 10 AND age != 29 AND age < 30 ALLOW FILTERING;");
        rows = getIndexed(store, 10, expressions);
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testHavingNonIndexedColumnsInTheClause()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer height = UTF8Type.instance.decompose("height");

        RowMutation rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm.add(CF_NAME, firstName, AsciiType.instance.decompose("pavel"), System.currentTimeMillis());
        rm.add(CF_NAME, height, Int32Type.instance.decompose(10), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm.add(CF_NAME, firstName, AsciiType.instance.decompose("pavel"), System.currentTimeMillis());
        rm.add(CF_NAME, height, Int32Type.instance.decompose(20), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key3"));
        rm.add(CF_NAME, firstName, AsciiType.instance.decompose("pavel"), System.currentTimeMillis());
        rm.add(CF_NAME, height, Int32Type.instance.decompose(30), System.currentTimeMillis());
        rm.apply();

        store.forceBlockingFlush();

        Set<String> rows;

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(height, IndexOperator.EQ, Int32Type.instance.decompose(10)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(height, IndexOperator.GT, Int32Type.instance.decompose(10)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(height, IndexOperator.GT, Int32Type.instance.decompose(10)),
                                     new IndexExpression(height, IndexOperator.LT, Int32Type.instance.decompose(30)));

        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a")),
                                     new IndexExpression(height, IndexOperator.GT, Int32Type.instance.decompose(10)),
                                     new IndexExpression(height, IndexOperator.LT, Int32Type.instance.decompose(30)),
                                     new IndexExpression(height, IndexOperator.NOT_EQ, Int32Type.instance.decompose(20)));

        Assert.assertEquals(0, rows.size());
    }

    @Test
    public void testUnicodeSupport()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("comment");

        RowMutation rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("ⓈⓅⒺⒸⒾⒶⓁ ⒞⒣⒜⒭⒮ and normal ones"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("龍馭鬱"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key3"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("インディアナ"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key4"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("レストラン"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key5"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("ベンジャミン ウエスト"), System.currentTimeMillis());
        rm.apply();


        Set<String> rows;

        /* Memtable */

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ⓈⓅⒺⒸⒾ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("normal")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("龍")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("龍馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ベンジャミン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key5" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("レストラ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("インディ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ベンジャミ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key5" }, rows.toArray(new String[rows.size()])));

        store.forceBlockingFlush();

        /* OnDiskSA */

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ⓈⓅⒺⒸⒾ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("normal")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("龍")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("龍馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ベンジャミン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key5" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("レストラ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("インディ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ベンジャミ")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key5" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testUnicodeSuffixMode()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("comment_suffix_split");

        RowMutation rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("龍馭鬱"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("インディアナ"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key3"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("レストラン"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key4"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("ベンジャミン ウエスト"), System.currentTimeMillis());
        rm.apply();


        Set<String> rows;

        /* Memtable */

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("龍")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("龍馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ベンジャミン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("トラン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ディア")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ジャミン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));

        store.forceBlockingFlush();

        /* OnDiskSA */

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("龍")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("龍馭鬱")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ベンジャミン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("トラン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ディア")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ジャミン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("ン")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testThatTooBigValueIsRejected()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("comment_suffix_split");

        for (int i = 0; i < 10; i++)
        {
            byte[] randomBytes = new byte[ThreadLocalRandom.current().nextInt(OnDiskIndexBuilder.MAX_TERM_SIZE, 5 * OnDiskIndexBuilder.MAX_TERM_SIZE)];
            ThreadLocalRandom.current().nextBytes(randomBytes);

            final ByteBuffer bigValue = UTF8Type.instance.decompose(new String(randomBytes));

            RowMutation rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
            rm.add(CF_NAME, comment, bigValue, System.currentTimeMillis());
            rm.apply();

            Set<String> rows;

            rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, bigValue.duplicate()));
            Assert.assertEquals(0, rows.size());

            store.forceBlockingFlush();

            rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, bigValue.duplicate()));
            Assert.assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSearchTimeouts() throws Exception
    {
        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");

        Map<String, Pair<String, Integer>> data1 = new HashMap<String, Pair<String, Integer>>()
        {{
                put("key1", Pair.create("Pavel", 14));
                put("key2", Pair.create("Pavel", 26));
                put("key3", Pair.create("Pavel", 27));
                put("key4", Pair.create("Jason", 27));
        }};

        ColumnFamilyStore store = loadData(data1, true);

        SSTableAttachedSecondaryIndex backend = (SSTableAttachedSecondaryIndex) store.indexManager.getIndexForColumn(UTF8Type.instance.decompose("first_name"));

        ExtendedFilter filter = ExtendedFilter.create(store,
                                                      DataRange.allData(StorageService.getPartitioner()),
                                                      Collections.singletonList(new IndexExpression(firstName, IndexOperator.EQ, UTF8Type.instance.decompose("a"))),
                                                      100,
                                                      false,
                                                      System.currentTimeMillis());

        try
        {
            new QueryPlan(backend, filter, 0).execute();
            Assert.fail();
        }
        catch (TimeQuotaExceededException e)
        {
            // correct behavior
        }
        catch (Exception e)
        {
            Assert.fail();
            e.printStackTrace();
        }

        // to make sure that query doesn't fail in normal conditions

        Set<String> rows = getKeys(new QueryPlan(backend, filter, DatabaseDescriptor.getRangeRpcTimeout()).execute());
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2", "key3", "key4" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testLowerCaseAnalyzer()
    {
        testLowerCaseAnalyzer(false);
        cleanupData();
        testLowerCaseAnalyzer(true);
    }

    @Test
    public void testChinesePrefixSearch()
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer fullName = UTF8Type.instance.decompose("/output/full-name/");

        RowMutation rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm.add(CF_NAME, fullName, UTF8Type.instance.decompose("美加 八田"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm.add(CF_NAME, fullName, UTF8Type.instance.decompose("仁美 瀧澤"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key3"));
        rm.add(CF_NAME, fullName, UTF8Type.instance.decompose("晃宏 高須"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key4"));
        rm.add(CF_NAME, fullName, UTF8Type.instance.decompose("弘孝 大竹"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key5"));
        rm.add(CF_NAME, fullName, UTF8Type.instance.decompose("満枝 榎本"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key6"));
        rm.add(CF_NAME, fullName, UTF8Type.instance.decompose("飛鳥 上原"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key7"));
        rm.add(CF_NAME, fullName, UTF8Type.instance.decompose("大輝 鎌田"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key8"));
        rm.add(CF_NAME, fullName, UTF8Type.instance.decompose("利久 寺地"), System.currentTimeMillis());
        rm.apply();

        store.forceBlockingFlush();


        Set<String> rows;

        rows = getIndexed(store, 10, new IndexExpression(fullName, IndexOperator.EQ, UTF8Type.instance.decompose("美加 八田")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(fullName, IndexOperator.EQ, UTF8Type.instance.decompose("美加")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(fullName, IndexOperator.EQ, UTF8Type.instance.decompose("晃宏 高須")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(fullName, IndexOperator.EQ, UTF8Type.instance.decompose("大輝")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key7" }, rows.toArray(new String[rows.size()])));
    }

    public void testLowerCaseAnalyzer(boolean forceFlush)
    {
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer comment = UTF8Type.instance.decompose("address");

        RowMutation rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("577 Rogahn Valleys Apt. 178"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("89809 Beverly Course Suite 089"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key3"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("165 clydie oval apt. 399"), System.currentTimeMillis());
        rm.apply();

        if (forceFlush)
            store.forceBlockingFlush();

        Set<String> rows;

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("577 Rogahn Valleys")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("577 ROgAhn VallEYs")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("577 rogahn valleys")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("577 rogahn")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("57")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("89809 Beverly Course")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("89809 BEVERly COURSE")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("89809 beverly course")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("89809 Beverly")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("8980")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("165 ClYdie OvAl APT. 399")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("165 Clydie Oval Apt. 399")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("165 clydie oval apt. 399")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("165 ClYdie OvA")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("165 ClYdi")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(comment, IndexOperator.EQ, UTF8Type.instance.decompose("165")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testPrefixSSTableLookup()
    {
        // This test coverts particular case which interval lookup can return invalid results
        // when queried on the prefix e.g. "j".
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer name = UTF8Type.instance.decompose("first_name_prefix");

        RowMutation rm;

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm.add(CF_NAME, name, UTF8Type.instance.decompose("Pavel"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm.add(CF_NAME, name, UTF8Type.instance.decompose("Jordan"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key3"));
        rm.add(CF_NAME, name, UTF8Type.instance.decompose("Mikhail"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key4"));
        rm.add(CF_NAME, name, UTF8Type.instance.decompose("Michael"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key5"));
        rm.add(CF_NAME, name, UTF8Type.instance.decompose("Johnny"), System.currentTimeMillis());
        rm.apply();

        // first flush would make interval for name - 'johnny' -> 'pavel'
        store.forceBlockingFlush();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key6"));
        rm.add(CF_NAME, name, UTF8Type.instance.decompose("Jason"), System.currentTimeMillis());
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key7"));
        rm.add(CF_NAME, name, UTF8Type.instance.decompose("Vijay"), System.currentTimeMillis());
        rm.apply();

        // this flush is going to produce range - 'jason' -> 'vijay'
        store.forceBlockingFlush();

        // make sure that overlap of the prefixes is properly handled across sstables
        // since simple interval tree lookup is not going to cover it, prefix lookup actually required.

        Set<String> rows;
        rows = getIndexed(store, 10, new IndexExpression(name, IndexOperator.EQ, UTF8Type.instance.decompose("J")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key5", "key6" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(name, IndexOperator.EQ, UTF8Type.instance.decompose("j")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key5", "key6" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(name, IndexOperator.EQ, UTF8Type.instance.decompose("m")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3", "key4" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(name, IndexOperator.EQ, UTF8Type.instance.decompose("v")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key7" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(name, IndexOperator.EQ, UTF8Type.instance.decompose("p")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, new IndexExpression(name, IndexOperator.EQ, UTF8Type.instance.decompose("j")),
                                     new IndexExpression(name, IndexOperator.NOT_EQ, UTF8Type.instance.decompose("joh")));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2", "key6" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testNotEqualsWithMultiTermSearch() throws Exception
    {
        final long now = System.currentTimeMillis();

        // This test coverts particular case which interval lookup can return invalid results
        // when queried on the prefix e.g. "j".
        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        final ByteBuffer firstName = UTF8Type.instance.decompose("first_name");
        final ByteBuffer comment = UTF8Type.instance.decompose("comment");
        final ByteBuffer height = UTF8Type.instance.decompose("height");

        RowMutation rm;

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key1"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("software engineer works on distributed systems"), now);
        rm.add(CF_NAME, height, Int32Type.instance.decompose(175), now);
        rm.add(CF_NAME, firstName, UTF8Type.instance.decompose("j"), now);
        rm.apply();

        store.forceBlockingFlush();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm.add(CF_NAME, comment, UTF8Type.instance.decompose("software engineer likes distributed systems"), now);
        rm.add(CF_NAME, height, Int32Type.instance.decompose(176), now);
        rm.add(CF_NAME, firstName, UTF8Type.instance.decompose("j"), now);
        rm.apply();

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key3"));
        rm.add(CF_NAME, firstName, UTF8Type.instance.decompose("j"), now);
        rm.apply();

        Set<String> rows;

        rows = getIndexed(store, 10, getExpressions("SELECT * FROM %s.%s WHERE comment = 'soft eng' ALLOW FILTERING;"));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, getExpressions("SELECT * FROM %s.%s WHERE comment = 'soft eng' AND comment != 'works' ALLOW FILTERING;"));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, getExpressions("SELECT * FROM %s.%s WHERE comment != 'works' AND comment = 'soft eng' ALLOW FILTERING;"));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, getExpressions("SELECT * FROM %s.%s WHERE comment = 'soft eng' and comment != 'work' and comment != 'likes' ALLOW FILTERING;"));
        Assert.assertTrue(rows.isEmpty());

        rows = getIndexed(store, 10, getExpressions("SELECT * FROM %s.%s WHERE comment != 'likes' and comment = 'soft eng' and comment != 'work' ALLOW FILTERING;"));
        Assert.assertTrue(rows.isEmpty());

        rows = getIndexed(store, 10, getExpressions("SELECT * FROM %s.%s WHERE comment = 'soft eng' and comment != 'work likes' ALLOW FILTERING;"));
        Assert.assertTrue(rows.isEmpty());

        rows = getIndexed(store, 10, getExpressions("SELECT * FROM %s.%s WHERE comment = 'soft eng' and height != 175 ALLOW FILTERING;"));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key2" }, rows.toArray(new String[rows.size()])));

        rows = getIndexed(store, 10, getExpressions("SELECT * FROM %s.%s WHERE comment = 'soft eng' and height != 175 and height != 176 ALLOW FILTERING;"));
        Assert.assertTrue(rows.isEmpty());

        rows = executeCQL(String.format("SELECT * FROM %s.%s WHERE first_name = 'j' AND comment != 'soft eng' ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key3" }, rows.toArray(new String[rows.size()])));

        rows = executeCQL(String.format("SELECT * FROM %s.%s WHERE first_name = 'j' AND comment = 'soft eng' ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1", "key2" }, rows.toArray(new String[rows.size()])));

        // let's make sure that index filtering on deleted columns returns proper results, since index is
        // still going to match key2 => 'soft eng' as it's live as far as index is concerned.

        rm = new RowMutation(KS_NAME, AsciiType.instance.decompose("key2"));
        rm.delete(CF_NAME, comment, now + 1);
        rm.apply();

        rows = executeCQL(String.format("SELECT * FROM %s.%s WHERE first_name = 'j' AND comment = 'soft eng' ALLOW FILTERING;", KS_NAME, CF_NAME));
        Assert.assertTrue(rows.toString(), Arrays.equals(new String[] { "key1" }, rows.toArray(new String[rows.size()])));
    }

    @Test
    public void testSettingIsLiteralOption()
    {
        // special type which is UTF-8 but is only on the inside
        AbstractType<?> stringType = new AbstractType<String>()
        {
            @Override
            public ByteBuffer fromString(String source) throws MarshalException
            {
                return UTF8Type.instance.fromString(source);
            }

            @Override
            public TypeSerializer<String> getSerializer()
            {
                return UTF8Type.instance.getSerializer();
            }

            @Override
            public int compare(ByteBuffer a, ByteBuffer b)
            {
                return UTF8Type.instance.compare(a, b);
            }
        };

        // first let's check that we get 'false' for 'isLiteral' if we don't set the option with special comparator
        ColumnDefinition columnA = new ColumnDefinition(UTF8Type.instance.fromString("special-A"),
                                                        stringType,
                                                        IndexType.CUSTOM,
                                                        new HashMap<String, String>()
                                                        {{
                                                            put(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, SSTableAttachedSecondaryIndex.class.getName());
                                                        }},
                                                        "special-index-A",
                                                        null,
                                                        ColumnDefinition.Type.REGULAR);

        ColumnIndex indexA = new ColumnIndex(UTF8Type.instance, columnA, UTF8Type.instance);

        Assert.assertEquals(true,  indexA.isIndexed());
        Assert.assertEquals(false, indexA.isLiteral());

        // now let's double-check that we do get 'true' when we set it
        ColumnDefinition columnB = new ColumnDefinition(UTF8Type.instance.fromString("special-B"),
                                                        stringType,
                                                        IndexType.CUSTOM,
                                                        new HashMap<String, String>()
                                                        {{
                                                            put(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, SSTableAttachedSecondaryIndex.class.getName());
                                                            put("is_literal", "true");
                                                        }},
                                                        "special-index-B",
                                                        null,
                                                        ColumnDefinition.Type.REGULAR);

        ColumnIndex indexB = new ColumnIndex(UTF8Type.instance, columnB, UTF8Type.instance);

        Assert.assertEquals(true, indexB.isIndexed());
        Assert.assertEquals(true, indexB.isLiteral());

        // and finally we should also get a 'true' if it's built-in UTF-8/ASCII comparator
        ColumnDefinition columnC = new ColumnDefinition(UTF8Type.instance.fromString("special-C"),
                                                        UTF8Type.instance,
                                                        IndexType.CUSTOM,
                                                        new HashMap<String, String>()
                                                        {{
                                                            put(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, SSTableAttachedSecondaryIndex.class.getName());
                                                        }},
                                                        "special-index-C",
                                                        null,
                                                        ColumnDefinition.Type.REGULAR);

        ColumnIndex indexC = new ColumnIndex(UTF8Type.instance, columnC, UTF8Type.instance);

        Assert.assertEquals(true, indexC.isIndexed());
        Assert.assertEquals(true, indexC.isLiteral());

        ColumnDefinition columnD = new ColumnDefinition(UTF8Type.instance.fromString("special-D"),
                                                        AsciiType.instance,
                                                        IndexType.CUSTOM,
                                                        new HashMap<String, String>()
                                                        {{
                                                            put(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, SSTableAttachedSecondaryIndex.class.getName());
                                                        }},
                                                        "special-index-D",
                                                        null,
                                                        ColumnDefinition.Type.REGULAR);

        ColumnIndex indexD = new ColumnIndex(UTF8Type.instance, columnD, UTF8Type.instance);

        Assert.assertEquals(true, indexD.isIndexed());
        Assert.assertEquals(true, indexD.isLiteral());

        // and option should supersedes the comparator type
        ColumnDefinition columnE = new ColumnDefinition(UTF8Type.instance.fromString("special-E"),
                                                        UTF8Type.instance,
                                                        IndexType.CUSTOM,
                                                        new HashMap<String, String>()
                                                        {{
                                                            put(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, SSTableAttachedSecondaryIndex.class.getName());
                                                            put("is_literal", "false");
                                                        }},
                                                        "special-index-E",
                                                        null,
                                                        ColumnDefinition.Type.REGULAR);

        ColumnIndex indexE = new ColumnIndex(UTF8Type.instance, columnE, UTF8Type.instance);

        Assert.assertEquals(true,  indexE.isIndexed());
        Assert.assertEquals(false, indexE.isLiteral());

    }

    private static IndexExpression[] getExpressions(String cqlQuery) throws Exception
    {
        ParsedStatement parsedStatement = QueryProcessor.parseStatement(String.format(cqlQuery, KS_NAME, CF_NAME));
        SelectStatement selectStatement = (SelectStatement) parsedStatement.prepare().statement;

        List<IndexExpression> expressions = selectStatement.getIndexExpressions(Collections.<ByteBuffer>emptyList());
        return expressions.toArray(new IndexExpression[expressions.size()]);
    }

    private static ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data, boolean forceFlush)
    {
        return loadData(data, System.currentTimeMillis(), forceFlush);
    }

    private static ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data, long timestamp, boolean forceFlush)
    {
        for (Map.Entry<String, Pair<String, Integer>> e : data.entrySet())
            newMutation(e.getKey(), e.getValue().left, null, e.getValue().right, timestamp).apply();

        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);

        if (forceFlush)
            store.forceBlockingFlush();

        return store;
    }

    private void cleanupData()
    {
        Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).truncateBlocking();
    }

    private static Set<String> getIndexed(ColumnFamilyStore store, int maxResults, IndexExpression... expressions)
    {
        return getIndexed(store, new IdentityQueryFilter(), maxResults, expressions);
    }

    private static Set<String> getIndexed(ColumnFamilyStore store, IDiskAtomFilter columnFilter, int maxResults, IndexExpression... expressions)
    {
        return getKeys(getIndexed(store, columnFilter, null, maxResults, expressions));
    }

    private static Set<DecoratedKey> getPaged(ColumnFamilyStore store, int pageSize, IndexExpression... expressions)
    {
        List<Row> currentPage;
        Set<DecoratedKey> uniqueKeys = new TreeSet<>();

        DecoratedKey lastKey = null;
        do
        {
            currentPage = getIndexed(store, new IdentityQueryFilter(), lastKey, pageSize, expressions);

            if (currentPage == null)
                break;

            for (Row row : currentPage)
                uniqueKeys.add(row.key);

            Row lastRow = Iterators.getLast(currentPage.iterator(), null);
            if (lastRow == null)
                break;

            lastKey = lastRow.key;
        }
        while (currentPage.size() == pageSize);

        return uniqueKeys;
    }

    private static List<Row> getIndexed(ColumnFamilyStore store, IDiskAtomFilter columnFilter, DecoratedKey startKey, int maxResults, IndexExpression... expressions)
    {
        IPartitioner p = StorageService.getPartitioner();
        AbstractBounds<RowPosition> bounds;

        if (startKey == null)
        {
            bounds = new Range<>(p.getMinimumToken(), p.getMinimumToken()).toRowBounds();
        }
        else
        {
            bounds = new Bounds<>(startKey, p.getMinimumToken().maxKeyBound(p));
        }

        return store.indexManager.search(ExtendedFilter.create(store,
                                         new DataRange(bounds, columnFilter),
                                         Arrays.asList(expressions),
                                         maxResults,
                                         false,
                                         System.currentTimeMillis()));
    }

    private static RowMutation newMutation(String key, String firstName, String lastName, int age, long timestamp)
    {
        RowMutation rm = new RowMutation(KS_NAME, AsciiType.instance.decompose(key));
        if (firstName != null)
            rm.add(CF_NAME, ByteBufferUtil.bytes("first_name"), UTF8Type.instance.decompose(firstName), timestamp);
        if (lastName != null)
            rm.add(CF_NAME, ByteBufferUtil.bytes("last_name"), UTF8Type.instance.decompose(lastName), timestamp);
        if (age >= 0)
            rm.add(CF_NAME, ByteBufferUtil.bytes("age"), Int32Type.instance.decompose(age), timestamp);

        return rm;
    }

    private static Set<String> getKeys(final List<Row> rows)
    {
        return new TreeSet<String>()
        {{
            for (Row r : rows)
                add(AsciiType.instance.compose(r.key.key));
        }};
    }

    private static List<String> convert(final Set<DecoratedKey> keys)
    {
        return new ArrayList<String>()
        {{
            for (DecoratedKey key : keys)
                add(AsciiType.instance.getString(key.key));
        }};
    }

    private Set<String> executeCQL(String rawStatement) throws Exception
    {
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(rawStatement).prepare().statement;
        ResultMessage.Rows cqlRows = statement.executeInternal(QueryState.forInternalCalls(), new QueryOptions(ConsistencyLevel.LOCAL_ONE, Collections.<ByteBuffer>emptyList()));

        Set<String> results = new TreeSet<>();
        for (CqlRow row : cqlRows.toThriftResult().getRows())
        {
            for (org.apache.cassandra.thrift.Column col : row.columns)
            {
                String columnName = UTF8Type.instance.getString(col.bufferForName());
                if (columnName.equals("key"))
                    results.add(AsciiType.instance.getString(col.bufferForValue()));
            }
        }

        return results;
    }
}
