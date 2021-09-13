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

import com.codahale.metrics.Counter;
import com.mware.core.config.Configuration;
import com.mware.core.config.options.CoreOptions;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.WorkerSpout;
import com.mware.core.ingest.WorkerTuple;
import com.mware.core.ingest.dataworker.WorkerItem;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.status.MetricsManager;
import com.mware.core.status.StatusServer;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

public abstract class WorkerBase<TWorkerItem extends WorkerItem> {
    private final boolean statusEnabled;
    private final boolean exitOnNextTupleFailure;
    private final Counter queueSizeMetric;
    private final MetricsManager metricsManager;
    private final String queueSizeMetricName;
    private WorkQueueRepository workQueueRepository;
    private WebQueueRepository webQueueRepository;
    private volatile boolean shouldRun;
    private StatusServer statusServer = null;
    private final Queue<WorkerItemWrapper> tupleQueue = new LinkedList<>();
    private final int tupleQueueSize;
    private Thread processThread;

    protected WorkerBase(
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            Configuration configuration,
            MetricsManager metricsManager
    ) {
        this.workQueueRepository = workQueueRepository;
        this.webQueueRepository = webQueueRepository;
        this.metricsManager = metricsManager;
        this.exitOnNextTupleFailure = configuration.getBoolean(getClass().getName() + ".exitOnNextTupleFailure", true);
        this.tupleQueueSize = configuration.getInt(getClass().getName() + ".tupleQueueSize", 10);
        this.statusEnabled = configuration.get(CoreOptions.STATUS_ENABLED);
        this.queueSizeMetricName = metricsManager.getNamePrefix(this) + "queue-size-" + Thread.currentThread().getId();
        this.queueSizeMetric = metricsManager.counter(queueSizeMetricName);
    }

    @Override
    protected void finalize() throws Throwable {
        metricsManager.removeMetric(queueSizeMetricName);
        super.finalize();
    }

    public void run() throws Exception {
        BcLogger logger = BcLoggerFactory.getLogger(this.getClass());

        logger.info("begin runner");
        WorkerSpout workerSpout = prepareWorkerSpout();
        shouldRun = true;
        if (statusEnabled) {
            statusServer = createStatusServer();
        }
        startProcessThread(logger, workerSpout);
        pollWorkerSpout(logger, workerSpout);
        logger.info("end runner");
    }

    private void startProcessThread(BcLogger logger, WorkerSpout workerSpout) {
        processThread = new Thread(() -> {
            while (shouldRun) {
                WorkerItemWrapper workerItemWrapper = null;
                try {
                    synchronized (tupleQueue) {
                        do {
                            while (shouldRun && tupleQueue.size() == 0) {
                                tupleQueue.wait();
                            }
                            if (!shouldRun) {
                                return;
                            }
                            if (tupleQueue.size() > 0) {
                                workerItemWrapper = tupleQueue.remove();
                                queueSizeMetric.dec();
                                tupleQueue.notifyAll();
                            }
                        } while (shouldRun && workerItemWrapper == null);
                    }
                } catch (Exception ex) {
                    throw new BcException("Could not get next workerItem", ex);
                }
                if (!shouldRun) {
                    return;
                }
                try {
                    logger.debug("start processing");
                    long startTime = System.currentTimeMillis();
                    process(workerItemWrapper.getWorkerItem());
                    long endTime = System.currentTimeMillis();
                    logger.debug("completed processing in (%dms)", endTime - startTime);
                    workerSpout.ack(workerItemWrapper.getWorkerTuple());
                } catch (Throwable ex) {
                    logger.error("Could not process tuple: %s", workerItemWrapper, ex);
                    workerSpout.fail(workerItemWrapper.getWorkerTuple());
                }
            }
        });
        processThread.setName(Thread.currentThread().getName() + "-process");
        processThread.start();
    }

    private void pollWorkerSpout(BcLogger logger, WorkerSpout workerSpout) throws InterruptedException {
        while (shouldRun) {
            WorkerItemWrapper workerItemWrapper;
            WorkerTuple tuple = null;
            try {
                tuple = workerSpout.nextTuple();
                if (tuple == null) {
                    workerItemWrapper = null;
                } else {
                    TWorkerItem workerItem = tupleDataToWorkerItem(tuple.getData());
                    workerItemWrapper = new WorkerItemWrapper(workerItem, tuple);
                }
            } catch (InterruptedException ex) {
                if (tuple != null) {
                    workerSpout.fail(tuple);
                }
                throw ex;
            } catch (Exception ex) {
                if (tuple != null) {
                    workerSpout.fail(tuple);
                }
                handleNextTupleException(logger, ex);
                continue;
            }
            if (workerItemWrapper == null) {
                continue;
            }
            synchronized (tupleQueue) {
                tupleQueue.add(workerItemWrapper);
                queueSizeMetric.inc();
                tupleQueue.notifyAll();
                while (shouldRun && tupleQueue.size() >= tupleQueueSize) {
                    tupleQueue.wait();
                }
            }
        }
    }

    protected void handleNextTupleException(BcLogger logger, Exception ex) throws InterruptedException {
        if (exitOnNextTupleFailure) {
            throw new BcException("Failed to get next tuple", ex);
        } else {
            logger.error("Failed to get next tuple", ex);
            Thread.sleep(10 * 1000);
        }
    }

    protected abstract StatusServer createStatusServer() throws Exception;

    protected abstract void process(TWorkerItem workerItem) throws Exception;

    /**
     * This method gets called in a different thread than {@link #process(WorkerItem)} this
     * allows an implementing class to prefetch data needed for processing.
     */
    protected abstract TWorkerItem tupleDataToWorkerItem(byte[] data);

    public void stop() {
        shouldRun = false;
        if (statusServer != null) {
            statusServer.shutdown();
        }
        synchronized (tupleQueue) {
            tupleQueue.notifyAll();
        }
        try {
            if (processThread != null) {
                processThread.join(10000);
            }
        } catch (InterruptedException e) {
            throw new BcException("Could not stop process thread: " + processThread.getName());
        }
    }

    protected WorkerSpout prepareWorkerSpout() {
        WorkerSpout spout = workQueueRepository.createWorkerSpout(getQueueName());
        spout.open();
        return spout;
    }

    protected abstract String getQueueName();

    protected WorkQueueRepository getWorkQueueRepository() {
        return workQueueRepository;
    }

    public WebQueueRepository getWebQueueRepository() {
        return webQueueRepository;
    }

    public boolean shouldRun() {
        return shouldRun;
    }

    private class WorkerItemWrapper {
        private final TWorkerItem workerItem;
        private final WorkerTuple workerTuple;

        public WorkerItemWrapper(TWorkerItem workerItem, WorkerTuple workerTuple) {
            this.workerItem = workerItem;
            this.workerTuple = workerTuple;
        }

        public Object getMessageId() {
            return workerTuple.getMessageId();
        }

        public WorkerTuple getWorkerTuple() {
            return workerTuple;
        }

        public TWorkerItem getWorkerItem() {
            return workerItem;
        }

        @Override
        public String toString() {
            return "WorkerItemWrapper{" +
                    "messageId=" + getMessageId() +
                    ", workerItem=" + workerItem +
                    '}';
        }
    }
}
