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
package com.mware.core.externalResource;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import org.json.JSONObject;
import com.mware.ge.Authorizations;
import com.mware.core.ingest.WorkerSpout;
import com.mware.core.ingest.WorkerTuple;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.status.MetricEntry;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

public abstract class QueueExternalResourceWorker extends ExternalResourceWorker {
    public static final String QUEUE_NAME_PREFIX = "externalResource-";
    private final AuthorizationRepository authorizationRepository;
    private WorkQueueRepository workQueueRepository;
    private UserRepository userRepository;
    private volatile boolean shouldRun;
    private Timer processingTimeTimer;
    private Counter totalProcessedCounter;
    private Counter totalErrorCounter;
    private Collection<MetricEntry> metrics;

    protected QueueExternalResourceWorker(AuthorizationRepository authorizationRepository) {
        this.authorizationRepository = authorizationRepository;
    }

    @Override
    protected void prepare(@SuppressWarnings("UnusedParameters") User user) {
        super.prepare(user);
        String namePrefix = getMetricsManager().getNamePrefix(this);
        this.totalProcessedCounter = getMetricsManager().counter(namePrefix + "total-processed");
        this.totalErrorCounter = getMetricsManager().counter(namePrefix + "total-errors");
        this.processingTimeTimer = getMetricsManager().timer(namePrefix + "processing-time");

        metrics = new ArrayList<>();
        metrics.add(new MetricEntry("totalProcessed", this.totalProcessedCounter));
        metrics.add(new MetricEntry("totalErrors", this.totalErrorCounter));
        metrics.add(new MetricEntry("processingTime", this.processingTimeTimer));
    }

    @Override
    protected void run() throws Exception {
        BcLogger logger = BcLoggerFactory.getLogger(this.getClass());

        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(getUserRepository().getSystemUser());

        WorkerSpout workerSpout = this.workQueueRepository.createWorkerSpout(getQueueName());
        workerSpout.open();

        shouldRun = true;
        while (shouldRun) {
            WorkerTuple tuple = workerSpout.nextTuple();
            if (tuple == null) {
                Thread.sleep(100);
                continue;
            }
            try (Timer.Context t = processingTimeTimer.time()) {
                long startTime = System.currentTimeMillis();
                JSONObject json = new JSONObject(new String(tuple.getData()));
                process(tuple.getMessageId(), json, authorizations);
                long endTime = System.currentTimeMillis();
                logger.debug("completed processing in (%dms)", endTime - startTime);
                workerSpout.ack(tuple);
                this.totalProcessedCounter.inc();
            } catch (Throwable ex) {
                logger.error("Could not process tuple: %s", tuple, ex);
                this.totalErrorCounter.inc();
                workerSpout.fail(tuple);
            }
        }
        logger.debug("end runner");
    }

    public void stop() {
        shouldRun = false;
    }

    protected abstract void process(Object messageId, JSONObject json, Authorizations authorizations) throws Exception;

    public abstract String getQueueName();

    @Inject
    public final void setWorkQueueRepository(WorkQueueRepository workQueueRepository) {
        this.workQueueRepository = workQueueRepository;
    }

    public WorkQueueRepository getWorkQueueRepository() {
        return workQueueRepository;
    }

    public UserRepository getUserRepository() {
        return userRepository;
    }

    @Inject
    public final void setUserRepository(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public Collection<MetricEntry> getMetrics() {
        return metrics;
    }
}
