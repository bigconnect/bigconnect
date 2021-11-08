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
package com.mware.core.model.workQueue;

import com.google.inject.Inject;
import com.mware.core.config.Configuration;
import com.mware.core.ingest.WorkerSpout;
import com.mware.core.ingest.WorkerTuple;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.ge.Graph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryWorkQueueRepository extends WorkQueueRepository {
    private static Map<String, List<byte[]>> queues = new HashMap<>();

    @Inject
    public InMemoryWorkQueueRepository(
            Graph graph,
            Configuration configuration,
            GraphAuthorizationRepository graphAuthorizationRepository
    ) {
        super(graph, configuration, graphAuthorizationRepository);
    }

    @Override
    public void pushOnQueue(String queueName, byte[] data, Priority priority) {
        LOGGER.debug("push on queue: %s: %s", queueName, new String(data));
        addToQueue(queueName, data, priority);
    }

    public synchronized void addToQueue(String queueName, byte[] data, Priority priority) {
        final List<byte[]> queue = getQueue(queueName);
        if (priority == Priority.HIGH) {
            queue.add(0, data);
        } else {
            queue.add(data);
        }
    }

    @Override
    public void flush() {
    }

    @Override
    public void format() {
        clearQueue();
    }

    @Override
    public WorkerSpout createWorkerSpout(String queueName) {
        final List<byte[]> queue = getQueue(queueName);
        return new WorkerSpout() {
            @Override
            public WorkerTuple nextTuple() throws Exception {
                synchronized (queue) {
                    if (queue.size() == 0) {
                        Thread.sleep(100);
                        return null;
                    }
                    byte[] entry = queue.remove(0);
                    if (entry == null) {
                        Thread.sleep(100);
                        return null;
                    }
                    return new WorkerTuple("", entry);
                }
            }
        };
    }

    @Override
    public int getDwQueueSize() {
        if (queues.get(getDwQueueName()) != null) {
            return queues.get(getDwQueueName()).size();
        }
        return 0;
    }

    @Override
    public int getLrpQueueSize() {
        if (queues.get(getLrpQueueName()) != null) {
            return queues.get(getLrpQueueName()).size();
        }
        return 0;
    }

    public static void clearQueue() {
        queues.clear();
    }

    @Override
    protected void deleteQueue(String queueName) {
        queues.remove(queueName);
    }

    public static List<byte[]> getQueue(String queueName) {
        List<byte[]> queue = queues.get(queueName);
        if (queue == null) {
            queue = new CopyOnWriteArrayList<>();
            queues.put(queueName, queue);
        }
        return queue;
    }
}
