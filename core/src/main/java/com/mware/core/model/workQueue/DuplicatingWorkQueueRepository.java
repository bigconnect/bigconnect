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
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.status.model.Status;
import com.mware.ge.Element;
import com.mware.ge.Graph;
import org.json.JSONObject;

import java.util.Map;

public class DuplicatingWorkQueueRepository extends WorkQueueRepository {
    // primary
    private final RabbitMQWorkQueueRepository internalWorkQueue;
    // secondary
    private final RabbitMQWorkQueueRepository externalWorkQueue;

    private final String internalDwQueueName;
    private final String externalDwQueueName;
    private final String internalLrpQueueName;
    private final String externalLrpQueueName;

    @Inject
    public DuplicatingWorkQueueRepository(
            Graph graph,
            Configuration configuration,
            LifeSupportService lifeSupportService
    ) {
        super(graph, configuration);

        this.internalDwQueueName = configuration.get(Configuration.DW_INTERNAL_QUEUE_NAME, DW_DEFAULT_INTERNAL_QUEUE_NAME);
        this.externalDwQueueName = configuration.get(Configuration.DW_EXTERNAL_QUEUE_NAME, DW_DEFAULT_EXTERNAL_QUEUE_NAME);
        this.internalLrpQueueName = configuration.get(Configuration.LRP_INTERNAL_QUEUE_NAME, LRP_DEFAULT_INTERNAL_QUEUE_NAME);
        this.externalLrpQueueName = configuration.get(Configuration.LRP_EXTERNAL_QUEUE_NAME, LRP_DEFAULT_EXTERNAL_QUEUE_NAME);

        this.externalWorkQueue = new RabbitMQWorkQueueRepository(graph, configuration);
        externalWorkQueue.setQueueName(externalDwQueueName);
        this.internalWorkQueue = new RabbitMQWorkQueueRepository(graph, configuration);
        internalWorkQueue.setQueueName(internalDwQueueName);

        lifeSupportService.add(internalWorkQueue);
        lifeSupportService.add(externalWorkQueue);
    }

    @Override
    public void pushOnQueue(String queueName, byte[] data, Priority priority) {
        internalWorkQueue.pushOnQueue(internalDwQueueName, data, priority);
        externalWorkQueue.pushOnQueue(externalDwQueueName, data, priority);
    }

    @Override
    public void pushLongRunningProcessQueue(JSONObject queueItem, Priority priority) {
        internalWorkQueue.pushOnQueue(internalLrpQueueName, queueItem, priority);
        externalWorkQueue.pushOnQueue(externalLrpQueueName, queueItem, priority);
    }

    @Override
    public void flush() {
        internalWorkQueue.flush();
        externalWorkQueue.flush();
    }

    @Override
    protected void deleteQueue(String queueName) {
        internalWorkQueue.deleteQueue(internalDwQueueName);
        externalWorkQueue.deleteQueue(externalDwQueueName);
    }

    @Override
    public WorkerSpout createWorkerSpout(String queueName) {
        return internalWorkQueue.createWorkerSpout(queueName);
    }

    @Override
    public Map<String, Status> getQueuesStatus() {
        return internalWorkQueue.getQueuesStatus();
    }

    @Override
    protected boolean canHandle(Element element, String propertyKey, String propertyName, ElementOrPropertyStatus status) {
        return true;
    }
}
