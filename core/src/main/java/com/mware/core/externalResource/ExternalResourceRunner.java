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

import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ExternalResourceRunner {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ExternalResourceRunner.class);
    private final Configuration config;
    private final User user;
    private List<RunningWorker> runningWorkers = new ArrayList<>();

    public ExternalResourceRunner(
            Configuration config,
            final User user
    ) {
        this.config = config;
        this.user = user;
    }

    public void startAllAndWait() {
        Collection<RunningWorker> runningWorkers = startAll();
        while (runningWorkers.size() > 0) {
            for (RunningWorker runningWorker : runningWorkers) {
                if (!runningWorker.getThread().isAlive()) {
                    LOGGER.error("found a dead thread: " + runningWorker.getThread().getName());
                    return;
                }

                try {
                    runningWorker.getThread().join(1000);
                } catch (InterruptedException e) {
                    LOGGER.error("join interrupted", e);
                    return;
                }
            }
        }
    }

    public Collection<RunningWorker> startAll() {
        runningWorkers = new ArrayList<>();

        Collection<ExternalResourceWorker> workers = InjectHelper.getInjectedServices(
                ExternalResourceWorker.class,
                config
        );
        for (final ExternalResourceWorker worker : workers) {
            runningWorkers.add(start(worker, user));
        }
        return runningWorkers;
    }

    private RunningWorker start(final ExternalResourceWorker worker, final User user) {
        worker.prepare(user);
        Thread t = new Thread(() -> {
            try {
                worker.run();
            } catch (Throwable ex) {
                LOGGER.error("Failed running external resource worker: " + worker.getClass().getName(), ex);
            }
        });
        t.setName("external-resource-worker-" + worker.getClass().getSimpleName() + "-" + t.getId());
        t.setDaemon(true);
        LOGGER.debug("starting external resource worker thread: %s", t.getName());
        t.start();
        return new RunningWorker(worker, t);
    }

    public void shutdown() {
        LOGGER.debug("Stopping ExternalResourceRunner...");
        for (RunningWorker worker : runningWorkers) {
            worker.shutdown();
        }

        LOGGER.debug("Stopped ExternalResourceRunner");
    }

    public static class RunningWorker {
        private final ExternalResourceWorker worker;
        private final Thread thread;

        public RunningWorker(ExternalResourceWorker worker, Thread thread) {
            this.worker = worker;
            this.thread = thread;
        }

        public ExternalResourceWorker getWorker() {
            return worker;
        }

        public Thread getThread() {
            return thread;
        }

        public void shutdown() {
            worker.stop();
        }
    }
}
