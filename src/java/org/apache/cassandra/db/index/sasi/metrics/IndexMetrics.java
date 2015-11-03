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
package org.apache.cassandra.db.index.sasi.metrics;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.LatencyMetrics;
import org.apache.cassandra.metrics.MetricNameFactory;

public class IndexMetrics
{
    public final Meter timeouts;
    public final Meter failedRequests;
    public final Meter requests;
    public final LatencyMetrics latency;

    public IndexMetrics(ColumnFamilyStore cfs)
    {
        IndexNameFactory nameFactory = new IndexNameFactory(cfs);

        timeouts = Metrics.newMeter(nameFactory.createMetricName("Timeouts"), "search timeouts", TimeUnit.SECONDS);
        failedRequests = Metrics.newMeter(nameFactory.createMetricName("FailedRequests"), "search failed requests", TimeUnit.SECONDS);
        requests = Metrics.newMeter(nameFactory.createMetricName("TotalRequests"), "search total requests", TimeUnit.SECONDS);
        latency = new LatencyMetrics(nameFactory, "Search");
    }

    public void markRequest()
    {
        requests.mark();
    }

    public void markFailure()
    {
        failedRequests.mark();
    }

    public void markTimeout()
    {
        timeouts.mark();
    }

    public void updateLatency(long value, TimeUnit unit)
    {
        latency.addNano(unit.toNanos(value));
    }

    private static class IndexNameFactory implements MetricNameFactory
    {
        private final String ksName;
        private final String cfName;

        public IndexNameFactory(ColumnFamilyStore cfs)
        {
            ksName = cfs.keyspace.getName();
            cfName = cfs.name;
        }

        @Override
        public MetricName createMetricName(String metricName)
        {
            String groupName = IndexMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=sasi");
            mbeanName.append(",keyspace=").append(ksName);
            mbeanName.append(",scope=").append(cfName);
            mbeanName.append(",name=").append(metricName);

            return new MetricName(groupName, "sasi", metricName, ksName + "." + cfName, mbeanName.toString());
        }
    }
}
