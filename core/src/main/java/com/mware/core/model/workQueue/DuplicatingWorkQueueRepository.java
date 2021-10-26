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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.WorkerSpout;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.ge.Element;
import com.mware.ge.Graph;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Set;

import static com.mware.core.config.Configuration.DW_QUEUE_PREFIX;
import static com.mware.core.config.Configuration.LRP_QUEUE_PREFIX;

public class DuplicatingWorkQueueRepository extends WorkQueueRepository {
    // primary
    private final RabbitMQWorkQueueRepository workQueueRepository;

    private final Set<String> dwQueueNames;
    private final Set<String> lrpQueueNames;

    @Inject
    public DuplicatingWorkQueueRepository(
            Graph graph,
            Configuration configuration,
            LifeSupportService lifeSupportService
    ) {
        super(graph, configuration);

        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (String key : configuration.getKeys(DW_QUEUE_PREFIX)) {
            if (key.endsWith(".name")) {
                String queue = configuration.get(key, null);
                builder.add(queue);
            }
        }
        dwQueueNames = builder.build();

        builder = ImmutableSet.builder();
        for (String key : configuration.getKeys(LRP_QUEUE_PREFIX)) {
            if (key.endsWith(".name")) {
                String queue = configuration.get(key, null);
                builder.add(queue);
            }
        }
        lrpQueueNames = builder.build();

        workQueueRepository = new RabbitMQWorkQueueRepository(graph, configuration, lifeSupportService);

        lifeSupportService.add(this);
    }

    @Override
    public void start() throws Throwable {
        dwQueueNames.forEach(q -> {
            try {
                workQueueRepository.ensureQueue(q);
            } catch (IOException e) {
                throw new BcException("Could not create queue", e);
            }
        });
    }

    @Override
    public void pushOnQueue(String queueName, byte[] data, Priority priority) {
        dwQueueNames.forEach(q -> workQueueRepository.pushOnQueue(q, data, priority));
    }

    @Override
    public void pushLongRunningProcessQueue(JSONObject queueItem, Priority priority) {
        lrpQueueNames.forEach(q -> workQueueRepository.pushOnQueue(q, queueItem, priority));
    }

    @Override
    public void flush() {
        workQueueRepository.flush();
    }

    @Override
    protected void deleteQueue(String queueName) {
    }

    @Override
    public WorkerSpout createWorkerSpout(String queueName) {
        return workQueueRepository.createWorkerSpout(queueName);
    }

    @Override
    public int getDwQueueSize() {
        return workQueueRepository.getDwQueueSize();
    }

    @Override
    public int getLrpQueueSize() {
        return workQueueRepository.getLrpQueueSize();
    }

    @Override
    protected boolean canHandle(Element element, String propertyKey, String propertyName, ElementOrPropertyStatus status) {
        return true;
    }
}
