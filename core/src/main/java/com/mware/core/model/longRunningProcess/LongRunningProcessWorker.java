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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import com.mware.core.status.MetricsManager;
import com.mware.core.status.StatusServer;
import com.mware.core.status.model.LongRunningProcessRunnerStatus;
import com.mware.core.status.model.Status;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import org.json.JSONObject;

public abstract class LongRunningProcessWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(LongRunningProcessWorker.class);
    private MetricsManager metricsManager;
    private Counter totalProcessedCounter;
    private Counter totalErrorCounter;
    private Counter processingCounter;
    private Timer processingTimeTimer;

    public void prepare(LongRunningWorkerPrepareData workerPrepareData) {
        String namePrefix = getMetricsManager().getNamePrefix(this);
        totalProcessedCounter = getMetricsManager().counter(namePrefix + "total-processed");
        processingCounter = getMetricsManager().counter(namePrefix + "processing");
        totalErrorCounter = getMetricsManager().counter(namePrefix + "total-errors");
        processingTimeTimer = getMetricsManager().timer(namePrefix + "processing-time");
    }

    public abstract boolean isHandled(JSONObject longRunningProcessQueueItem);

    public final void process(JSONObject longRunningProcessQueueItem) {
        try (Timer.Context t = processingTimeTimer.time()) {
            processingCounter.inc();
            try {
                processInternal(longRunningProcessQueueItem);
            } finally {
                processingCounter.dec();
            }
            totalProcessedCounter.inc();
        } catch (Throwable ex) {
            LOGGER.error("Failed to complete long running process: " + longRunningProcessQueueItem, ex);
            this.totalErrorCounter.inc();
            throw ex;
        }
    }

    protected abstract void processInternal(JSONObject longRunningProcessQueueItem);

    public LongRunningProcessRunnerStatus.LongRunningProcessWorkerStatus getStatus() {
        LongRunningProcessRunnerStatus.LongRunningProcessWorkerStatus status = new LongRunningProcessRunnerStatus.LongRunningProcessWorkerStatus();
        StatusServer.getGeneralInfo(status, getClass());
        status.getMetrics().put("totalProcessed", Status.Metric.create(totalProcessedCounter));
        status.getMetrics().put("processing", Status.Metric.create(processingCounter));
        status.getMetrics().put("totalErrors", Status.Metric.create(totalErrorCounter));
        status.getMetrics().put("processingTime", Status.Metric.create(processingTimeTimer));
        return status;
    }

    @Inject
    public final void setMetricsManager(MetricsManager metricsManager) {
        this.metricsManager = metricsManager;
    }

    public MetricsManager getMetricsManager() {
        return metricsManager;
    }

    public boolean systemPlugin() {
        return false;
    }
}
