/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
package com.mware.ge.accumulo;

import com.google.common.collect.Maps;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.commons.collections.MapUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(JUnit4.class)
public class AccumuloGraphConfigurationTest {

    @Test
    public void testBatchWriterConfigUsesDefaultWithNoParameters() {
        Map configMap = Maps.newHashMap();
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(configMap);
        BatchWriterConfig batchWriterConfig = accumuloGraphConfiguration.createBatchWriterConfig();

        assertThat(batchWriterConfig.getMaxLatency(TimeUnit.MILLISECONDS), is(AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_LATENCY));
        assertThat(batchWriterConfig.getTimeout(TimeUnit.MILLISECONDS), is(AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_TIMEOUT));
        assertThat(batchWriterConfig.getMaxMemory(), is(AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_MEMORY));
        assertThat(batchWriterConfig.getMaxWriteThreads(), is(AccumuloGraphConfiguration.DEFAULT_BATCHWRITER_MAX_WRITE_THREADS));
    }

    @Test
    public void testBatchWriterConfigIsSetToValuesWithParameters() {
        int numThreads = 2;
        long timeout = 3;
        long maxMemory = 5;
        long maxLatency = 7;

        Map configMap = Maps.newHashMap();
        MapUtils.putAll(configMap, new String[]{
                AccumuloGraphConfiguration.BATCHWRITER_MAX_LATENCY, "" + maxLatency,
                AccumuloGraphConfiguration.BATCHWRITER_MAX_MEMORY, "" + maxMemory,
                AccumuloGraphConfiguration.BATCHWRITER_MAX_WRITE_THREADS, "" + numThreads,
                AccumuloGraphConfiguration.BATCHWRITER_TIMEOUT, "" + timeout});
        AccumuloGraphConfiguration accumuloGraphConfiguration = new AccumuloGraphConfiguration(configMap);
        BatchWriterConfig batchWriterConfig = accumuloGraphConfiguration.createBatchWriterConfig();

        assertThat(batchWriterConfig.getMaxLatency(TimeUnit.MILLISECONDS), is(maxLatency));
        assertThat(batchWriterConfig.getTimeout(TimeUnit.MILLISECONDS), is(timeout));
        assertThat(batchWriterConfig.getMaxMemory(), is(maxMemory));
        assertThat(batchWriterConfig.getMaxWriteThreads(), is(numThreads));
    }
}
