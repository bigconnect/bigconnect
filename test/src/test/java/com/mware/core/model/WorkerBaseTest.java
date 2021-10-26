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
package com.mware.core.model;

import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.WorkerSpout;
import com.mware.core.ingest.dataworker.WorkerItem;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.util.BcLogger;
import com.mware.ge.metric.DropWizardMetricRegistry;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.metric.NullMetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WorkerBaseTest {
    private boolean stopOnNextTupleException;
    private int nextTupleExceptionCount;

    @Mock
    private WorkQueueRepository workQueueRepository;
    @Mock
    private WebQueueRepository webQueueRepository;
    @Mock
    private Configuration configuration;
    @Mock
    private WorkerSpout workerSpout;

    @Before
    public void before() {
        nextTupleExceptionCount = 0;
    }

    @Test
    public void testExitOnNextTupleFailure_exitOnNextTupleFailure_true() throws Exception {
        stopOnNextTupleException = false;
        when(configuration.getBoolean(eq(TestWorker.class.getName() + ".exitOnNextTupleFailure"), anyBoolean())).thenReturn(true);
        when(workQueueRepository.createWorkerSpout(eq("test"))).thenReturn(workerSpout);
        when(workerSpout.nextTuple()).thenThrow(new BcException("could not get nextTuple"));

        TestWorker testWorker = new TestWorker(workQueueRepository, webQueueRepository, configuration, new NullMetricRegistry());
        try {
            testWorker.run();
            fail("should throw");
        } catch (BcException ex) {
            assertEquals(1, nextTupleExceptionCount);
        }
    }

    @Test
    public void testExitOnNextTupleFailure_exitOnNextTupleFailure_false() throws Exception {
        stopOnNextTupleException = true;
        when(configuration.getBoolean(eq(TestWorker.class.getName() + ".exitOnNextTupleFailure"), anyBoolean())).thenReturn(false);
        when(workQueueRepository.createWorkerSpout(eq("test"))).thenReturn(workerSpout);
        when(workerSpout.nextTuple()).thenThrow(new BcException("could not get nextTuple"));

        TestWorker testWorker = new TestWorker(workQueueRepository, webQueueRepository, configuration, new NullMetricRegistry());
        testWorker.run();
        assertEquals(1, nextTupleExceptionCount);
    }

    private class TestWorker extends WorkerBase<TestWorkerItem> {
        protected TestWorker(WorkQueueRepository workQueueRepository, WebQueueRepository webQueueRepository, Configuration configuration, GeMetricRegistry metricRegistry) {
            super(workQueueRepository, webQueueRepository, configuration, metricRegistry);
        }

        @Override
        public TestWorkerItem tupleDataToWorkerItem(byte[] data) {
            return new TestWorkerItem(data);
        }

        @Override
        protected void process(TestWorkerItem workerItem) throws Exception {
            stop();
        }

        @Override
        protected String getQueueName() {
            return "test";
        }

        @Override
        protected void handleNextTupleException(BcLogger logger, Exception ex) throws InterruptedException {
            nextTupleExceptionCount++;
            if (stopOnNextTupleException) {
                stop();
                return;
            }
            super.handleNextTupleException(logger, ex);
        }
    }

    private class TestWorkerItem extends WorkerItem {
        private final byte[] data;

        public TestWorkerItem(byte[] data) {
            this.data = data;
        }
    }
}
