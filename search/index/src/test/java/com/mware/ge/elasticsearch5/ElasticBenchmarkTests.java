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
package com.mware.ge.elasticsearch5;

import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.GraphWithSearchIndex;
import com.mware.ge.Visibility;
import com.mware.ge.base.GraphBenchmarkTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.ge.util.GeAssert.fail;
import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assume.assumeTrue;

public class ElasticBenchmarkTests extends GraphBenchmarkTests implements GraphTestSetup {
    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(ElasticBenchmarkTests.class.getName());

    @Override
    public TestGraphFactory graphFactory() {
        return new ElasticGraphFactory().
                withElasticsearchResource(elasticsearchResource);
    }

    @Override
    public void before() throws Exception {
        elasticsearchResource.dropIndices();
        super.before();
    }

    @Override
    public boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(ElasticsearchOptions.INDEX_EDGES.name(), false);
        return true;
    }

    @Override
    public boolean isLuceneQueriesSupported() {
        return true;
    }

    @Override
    public boolean isPainlessDateMath() {
        return true;
    }

    @Test
    public void testMultipleThreadsFlushing() throws InterruptedException {
        assumeTrue(benchmarkEnabled());
        AtomicBoolean startSignal = new AtomicBoolean();
        AtomicBoolean run = new AtomicBoolean(true);
        AtomicBoolean writing = new AtomicBoolean(false);
        AtomicBoolean writeThenFlushComplete = new AtomicBoolean(false);
        CountDownLatch threadsReadyCountdown = new CountDownLatch(2);
        Runnable waitForStart = () -> {
            try {
                while (!startSignal.get()) {
                    synchronized (startSignal) {
                        threadsReadyCountdown.countDown();
                        startSignal.wait();
                    }
                }
            } catch (Exception ex) {
                throw new GeException("thread failed", ex);
            }
        };

        Thread constantWriteThread = new Thread(() -> {
            waitForStart.run();

            int i = 0;
            while (run.get()) {
                graph.prepareVertex("v" + i, new Visibility(""), CONCEPT_TYPE_THING)
                        .addPropertyValue("k1", "name1", stringValue("value1"), new Visibility(""))
                        .save(AUTHORIZATIONS_ALL);
                writing.set(true);
                i++;
            }
        });

        Thread writeThenFlushThread = new Thread(() -> {
            try {
                waitForStart.run();
                while (!writing.get()) {
                    Thread.sleep(10); // wait for other thread to start
                }

                for (int i = 0; i < 5; i++) {
                    graph.prepareVertex("vWriteTheFlush", new Visibility(""), CONCEPT_TYPE_THING)
                            .addPropertyValue("k1", "name1", stringValue("value1"), new Visibility(""))
                            .save(AUTHORIZATIONS_ALL);
                    graph.flush();
                }
                writeThenFlushComplete.set(true);
            } catch (Exception ex) {
                throw new GeException("thread failed", ex);
            }
        });

        // synchronize thread start
        constantWriteThread.start();
        writeThenFlushThread.start();
        threadsReadyCountdown.await();
        Thread.sleep(100);
        synchronized (startSignal) {
            startSignal.set(true);
            startSignal.notifyAll();
        }

        // wait to finish
        int timeout = 5000;
        long startTime = System.currentTimeMillis();
        while (!writeThenFlushComplete.get() && (System.currentTimeMillis() - startTime < timeout)) {
            Thread.sleep(10);
        }
        long endTime = System.currentTimeMillis();
        run.set(false);
        constantWriteThread.join();
        writeThenFlushThread.join();

        // check results
        if (endTime - startTime > timeout) {
            fail("timeout waiting for flush");
        }
    }
}
