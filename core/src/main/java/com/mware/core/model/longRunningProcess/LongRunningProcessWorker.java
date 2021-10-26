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
package com.mware.core.model.longRunningProcess;

import com.google.inject.Inject;
import com.mware.ge.Graph;
import com.mware.ge.metric.PausableTimerContext;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.metric.Counter;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.metric.Timer;
import org.json.JSONObject;

public abstract class LongRunningProcessWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(LongRunningProcessWorker.class);
    private GeMetricRegistry metricRegistry;
    private Counter totalProcessedCounter;
    private Counter totalErrorCounter;
    private Counter processingCounter;
    private Timer processingTimeTimer;

    public void prepare(LongRunningWorkerPrepareData workerPrepareData) {
        totalProcessedCounter = metricRegistry.getCounter(getClass(),  "total-processed");
        processingCounter = metricRegistry.getCounter(getClass(),  "processing");
        totalErrorCounter = metricRegistry.getCounter(getClass(),  "total-errors");
        processingTimeTimer = metricRegistry.getTimer(getClass(),  "processing-time");
    }

    public abstract boolean isHandled(JSONObject longRunningProcessQueueItem);

    public final void process(JSONObject longRunningProcessQueueItem) {
        PausableTimerContext t = new PausableTimerContext(processingTimeTimer);
        try {
            processingCounter.increment();
            try {
                processInternal(longRunningProcessQueueItem);
            } finally {
                processingCounter.decrement();
            }
            totalProcessedCounter.increment();
        } catch (Throwable ex) {
            LOGGER.error("Failed to complete long running process: " + longRunningProcessQueueItem, ex);
            this.totalErrorCounter.increment();
            throw ex;
        } finally {
            t.stop();
        }
    }

    protected abstract void processInternal(JSONObject longRunningProcessQueueItem);


    @Inject
    public void setMetricRegistry(Graph graph) {
        this.metricRegistry = graph.getMetricsRegistry();
    }
}
