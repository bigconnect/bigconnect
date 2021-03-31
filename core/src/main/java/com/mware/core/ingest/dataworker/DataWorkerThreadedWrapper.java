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
package com.mware.core.ingest.dataworker;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import com.mware.ge.Element;
import com.mware.core.exception.BcException;
import com.mware.core.status.MetricsManager;
import com.mware.core.status.PausableTimerContext;
import com.mware.core.status.PausableTimerContextAware;
import com.mware.core.status.StatusServer;
import com.mware.core.status.model.DataWorkerRunnerStatus;
import com.mware.core.status.model.Status;
import com.mware.core.trace.Trace;
import com.mware.core.trace.TraceSpan;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class DataWorkerThreadedWrapper implements Runnable {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(DataWorkerThreadedWrapper.class);
    private static final int DEQUEUE_TIMEOUT_MS = 30 * 1000;
    private static final int DEQUEUE_LOG_MESSAGE_FREQUENCY_MS = 10 * 1000;
    private static final int DEQUEUE_WARN_THRESHOLD_MS = 30 * 1000;
    private final DataWorker worker;

    public DataWorkerThreadedWrapper(DataWorker worker) {
        this.worker = worker;
    }

    private Counter totalProcessedCounter = null;
    private Counter processingCounter;
    private Counter totalErrorCounter;
    private Timer processingTimeTimer;
    private boolean stopped;
    private final Queue<Work> workItems = new LinkedList<>();
    private final Queue<WorkResult> workResults = new LinkedList<>();
    private MetricsManager metricsManager;

    @Override
    public final void run() {
        ensureMetricsInitialized();

        stopped = false;
        try {
            while (!stopped) {
                Work work;
                synchronized (workItems) {
                    if (workItems.size() == 0) {
                        workItems.wait(1000);
                        continue;
                    }
                    work = workItems.remove();
                }
                InputStream in = work.getIn();
                String workerClassName = this.worker.getClass().getName();
                Element element = work.getData() == null ? null : work.getData().getElement();
                String elementId = element == null ? null : element.getId();
                try {
                    LOGGER.debug("BEGIN doWork (%s): %s", workerClassName, elementId);
                    PausableTimerContext timerContext = new PausableTimerContext(processingTimeTimer);
                    if (in instanceof PausableTimerContextAware) {
                        ((PausableTimerContextAware) in).setPausableTimerContext(timerContext);
                    }
                    processingCounter.inc();
                    long startTime = System.currentTimeMillis();
                    TraceSpan traceSpan = startTraceIfEnabled(work, elementId);
                    try {
                        this.worker.execute(in, work.getData());
                    } finally {
                        stopTraceIfEnabled(work, traceSpan);
                        long endTime = System.currentTimeMillis();
                        long time = endTime - startTime;
                        LOGGER.debug("END doWork (%s): %s (%dms)", workerClassName, elementId, time);
                        processingCounter.dec();
                        totalProcessedCounter.inc();
                        timerContext.stop();
                    }
                    synchronized (workResults) {
                        workResults.add(new WorkResult(null));
                        workResults.notifyAll();
                    }
                } catch (Throwable ex) {
                    LOGGER.error("failed to complete work (%s): %s", workerClassName, elementId, ex);
                    totalErrorCounter.inc();
                    synchronized (workResults) {
                        workResults.add(new WorkResult(ex));
                        workResults.notifyAll();
                    }
                } finally {
                    try {
                        if (in != null) {
                            in.close();
                        }
                    } catch (IOException ex) {
                        synchronized (workResults) {
                            workResults.add(new WorkResult(ex));
                            workResults.notifyAll();
                        }
                    }
                }
            }
        } catch (InterruptedException ex) {
            LOGGER.error("thread was interrupted", ex);
        }
    }

    private void stopTraceIfEnabled(Work work, TraceSpan traceSpan) {
        if (work.getData().isTraceEnabled()) {
            if (traceSpan != null) {
                traceSpan.close();
            }
            Trace.off();
        }
    }

    private TraceSpan startTraceIfEnabled(Work work, String elementId) {
        TraceSpan traceSpan = null;
        if (work.getData().isTraceEnabled()) {
            Map<String, String> parameters = new HashMap<>();
            parameters.put("elementId", elementId);
            traceSpan = Trace.on("DW: " + this.worker.getClass().getName(), parameters);
        }
        return traceSpan;
    }

    private void ensureMetricsInitialized() {
        if (totalProcessedCounter == null) {
            String namePrefix = metricsManager.getNamePrefix(this.worker);
            totalProcessedCounter = metricsManager.counter(namePrefix + "total-processed");
            processingCounter = metricsManager.counter(namePrefix + "processing");
            totalErrorCounter = metricsManager.counter(namePrefix + "total-errors");
            processingTimeTimer = metricsManager.timer(namePrefix + "processing-time");
        }
    }

    public void enqueueWork(InputStream in, DataWorkerData data) {
        synchronized (workItems) {
            workItems.add(new Work(in, data));
            workItems.notifyAll();
        }
    }

    public WorkResult dequeueResult(boolean waitForever) {
        synchronized (workResults) {
            if (workResults.size() == 0) {
                Date startTime = new Date();
                Date lastMessageTime = new Date();
                while (workResults.size() == 0 && (waitForever || (getElapsedTime(startTime) < DEQUEUE_TIMEOUT_MS))) {
                    try {
                        if (getElapsedTime(lastMessageTime) > DEQUEUE_LOG_MESSAGE_FREQUENCY_MS) {
                            String message = String.format(
                                    "Worker \"%s\" has zero results. Waiting for results. (startTime: %s, elapsedTime: %ds, thread: %s)",
                                    worker.getClass().getName(),
                                    startTime,
                                    getElapsedTime(startTime) / 1000,
                                    Thread.currentThread().getName()
                            );
                            if (getElapsedTime(startTime) > DEQUEUE_WARN_THRESHOLD_MS) {
                                LOGGER.debug("%s", message);
                            }
                            lastMessageTime = new Date();
                        }
                        workResults.wait(1000);
                    } catch (InterruptedException ex) {
                        throw new BcException("Failed to wait for worker " + worker.getClass().getName(), ex);
                    }
                }
            }
            return workResults.remove();
        }
    }

    private long getElapsedTime(Date date) {
        return new Date().getTime() - date.getTime();
    }

    public void stop() {
        stopped = true;
    }

    public DataWorker getWorker() {
        return worker;
    }

    public DataWorkerRunnerStatus.DataWorkerStatus getStatus() {
        DataWorkerRunnerStatus.DataWorkerStatus status = new DataWorkerRunnerStatus.DataWorkerStatus();
        StatusServer.getGeneralInfo(status, this.worker.getClass());
        status.getMetrics().put("totalProcessed", Status.Metric.create(totalProcessedCounter));
        status.getMetrics().put("processing", Status.Metric.create(processingCounter));
        status.getMetrics().put("totalErrors", Status.Metric.create(totalErrorCounter));
        status.getMetrics().put("processingTime", Status.Metric.create(processingTimeTimer));
        return status;
    }

    private class Work {
        private final InputStream in;
        private final DataWorkerData data;

        public Work(InputStream in, DataWorkerData data) {
            this.in = in;
            this.data = data;
        }

        private InputStream getIn() {
            return in;
        }

        private DataWorkerData getData() {
            return data;
        }
    }

    public static class WorkResult {
        private final Throwable error;

        public WorkResult(Throwable error) {
            this.error = error;
        }

        public Throwable getError() {
            return error;
        }
    }

    @Inject
    public void setMetricsManager(MetricsManager metricsManager) {
        this.metricsManager = metricsManager;
    }

    @Override
    public String toString() {
        return "DataWorkerThreadedWrapper{" +
                "worker=" + worker +
                '}';
    }
}
