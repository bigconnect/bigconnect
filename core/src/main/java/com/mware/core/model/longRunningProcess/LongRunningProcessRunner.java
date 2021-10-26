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
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.model.WorkerBase;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.StoppableRunnable;
import com.mware.ge.Graph;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

// Unlike many other injected classes, this is not a singleton
public class LongRunningProcessRunner extends WorkerBase<LongRunningProcessWorkerItem> {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(LongRunningProcessRunner.class);
    private UserRepository userRepository;
    private LongRunningProcessRepository longRunningProcessRepository;
    private User user;
    private Configuration configuration;
    private List<LongRunningProcessWorker> workers = new ArrayList<>();

    @Inject
    public LongRunningProcessRunner(
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            Configuration configuration,
            Graph graph
    ) {
        super(workQueueRepository, webQueueRepository, configuration, graph.getMetricsRegistry());
    }

    public void prepare(Map map) {
        prepareUser(map);
        prepareWorkers(map);
    }

    private void prepareUser(Map map) {
        this.user = (User) map.get("user");
        if (this.user == null) {
            this.user = this.userRepository.getSystemUser();
        }
    }

    private void prepareWorkers(Map map) {
        LongRunningWorkerPrepareData workerPrepareData = new LongRunningWorkerPrepareData(
                map,
                this.user,
                InjectHelper.getInjector()
        );

        Collection<LongRunningProcessWorker> injectedServices =
                InjectHelper.getInjectedServices(LongRunningProcessWorker.class, configuration);

        for (LongRunningProcessWorker worker : injectedServices) {
            try {
                LOGGER.info("preparing: %s", worker.getClass().getName());
                worker.prepare(workerPrepareData);
            } catch (Exception ex) {
                throw new BcException("Could not prepare data worker " + worker.getClass().getName(), ex);
            }
            workers.add(worker);
        }
    }

    @Override
    protected LongRunningProcessWorkerItem tupleDataToWorkerItem(byte[] data) {
        return new LongRunningProcessWorkerItem(data);
    }

    @Override
    public void process(LongRunningProcessWorkerItem workerItem) {
        JSONObject longRunningProcessQueueItem = workerItem.getJson();
        LOGGER.info("Process long running queue item %s", longRunningProcessQueueItem.toString());

        LongRunningProcessWorker worker = workers.stream().filter(w -> w.isHandled(longRunningProcessQueueItem))
                .findFirst().orElse(null);

        if (worker == null) {
            LOGGER.debug("Could not find interested LRP workers.");
            return;
        }

        try {
            longRunningProcessQueueItem.put("startTime", System.currentTimeMillis());
            longRunningProcessQueueItem.put("progress", 0.0);
            longRunningProcessRepository.beginWork(longRunningProcessQueueItem);
            getWebQueueRepository().broadcastLongRunningProcessChange(longRunningProcessQueueItem);

            worker.process(longRunningProcessQueueItem);

            longRunningProcessQueueItem.put("endTime", System.currentTimeMillis());
            longRunningProcessQueueItem.put("progress", 1.0);
            longRunningProcessRepository.ack(longRunningProcessQueueItem);
            getWebQueueRepository().broadcastLongRunningProcessChange(longRunningProcessQueueItem);
        } catch (Throwable ex) {
            LOGGER.error("Failed to process long running process queue item", ex);
            longRunningProcessQueueItem.put("error", ex.getMessage());
            longRunningProcessQueueItem.put("endTime", System.currentTimeMillis());
            longRunningProcessRepository.nak(longRunningProcessQueueItem, ex);
            getWebQueueRepository().broadcastLongRunningProcessChange(longRunningProcessQueueItem);
        }
    }

    @Override
    protected String getQueueName() {
        return configuration.get(Configuration.LRP_QUEUE_NAME, WorkQueueRepository.LRP_DEFAULT_QUEUE_NAME);
    }

    @Inject
    public void setUserRepository(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Inject
    public void setLongRunningProcessRepository(LongRunningProcessRepository longRunningProcessRepository) {
        this.longRunningProcessRepository = longRunningProcessRepository;
    }

    @Inject
    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public static List<StoppableRunnable> startThreaded(int threadCount, Configuration config) {
        List<StoppableRunnable> stoppables = new ArrayList<>();

        LOGGER.info("Starting LongRunningProcessRunners on %d threads", threadCount);
        for (int i = 0; i < threadCount; i++) {
            StoppableRunnable stoppable = new StoppableRunnable() {
                private LongRunningProcessRunner longRunningProcessRunner = null;

                @Override
                public void run() {
                    try {
                        longRunningProcessRunner = InjectHelper.getInstance(LongRunningProcessRunner.class);
                        longRunningProcessRunner.prepare(config.toMap());
                        longRunningProcessRunner.run();
                    } catch (Exception ex) {
                        LOGGER.error("Failed running LongRunningProcessRunner", ex);
                    }
                }

                @Override
                public void stop() {
                    try {
                        if (longRunningProcessRunner != null) {
                            LOGGER.debug("Stopping LongRunningProcessRunner");
                            longRunningProcessRunner.stop();
                        }
                    } catch (Exception ex) {
                        LOGGER.error("Failed stopping LongRunningProcessRunner", ex);
                    }
                }
            };
            stoppables.add(stoppable);
            Thread t = new Thread(stoppable);
            t.setName("long-running-process-runner-" + t.getId());
            t.setDaemon(true);
            LOGGER.debug("Starting LongRunningProcessRunner thread: %s", t.getName());
            t.start();
        }

        return stoppables;
    }
}
