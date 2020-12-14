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

import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.WorkerSpout;
import com.mware.core.ingest.dataworker.DataWorkerMessage;
import com.mware.core.ingest.dataworker.DataWorkerRunner;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.lifecycle.LifecycleAdapter;
import com.mware.core.model.properties.types.BcPropertyUpdate;
import com.mware.core.model.properties.types.BcPropertyUpdateRemove;
import com.mware.core.status.model.Status;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Edge;
import com.mware.ge.Element;
import com.mware.ge.Graph;
import com.mware.ge.Vertex;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class WorkQueueRepository extends LifecycleAdapter {
    public static final String DW_DEFAULT_INTERNAL_QUEUE_NAME = "intDataWorker";
    public static final String DW_DEFAULT_EXTERNAL_QUEUE_NAME = "extDataWorker";
    public static final String LRP_DEFAULT_INTERNAL_QUEUE_NAME = "intLongRunningProcess";
    public static final String LRP_DEFAULT_EXTERNAL_QUEUE_NAME = "extLongRunningProcess";

    protected static final BcLogger LOGGER = BcLoggerFactory.getLogger(WorkQueueRepository.class);
    private final Configuration configuration;
    protected String queueName;
    private final Graph graph;
    private DataWorkerRunner dataWorkerRunner;

    protected WorkQueueRepository(
            Graph graph,
            Configuration configuration
    ) {
        this.graph = graph;
        this.configuration = configuration;
        setQueueName(configuration.get(Configuration.DW_INTERNAL_QUEUE_NAME, DW_DEFAULT_INTERNAL_QUEUE_NAME));
    }

    public void pushGraphPropertyQueue(
            Element element,
            String propertyKey,
            String propertyName,
            String workspaceId,
            String visibilitySource,
            Priority priority,
            ElementOrPropertyStatus status,
            Long beforeDeleteTimestamp
    ) {
        getGraph().flush();
        checkNotNull(element);

        LOGGER.debug("pushGraphPropertyQueue: %s", element.getId());

        DataWorkerMessage data = createPropertySpecificMessage(
                propertyKey,
                propertyName,
                workspaceId,
                visibilitySource,
                status,
                beforeDeleteTimestamp,
                priority
        );

        addElementTypeToJson(data, element);

        if (canHandle(element, propertyKey, propertyName, status)) {
            pushOnQueue(queueName, data.toBytes(), priority);
        }
    }

    public void pushGraphPropertyQueue(
            Element element,
            Iterable<BcPropertyUpdate> properties,
            String workspaceId,
            String visibilitySource,
            Priority priority
    ) {
        DataWorkerMessage data = new DataWorkerMessage();
        data.setPriority(priority);

        List<DataWorkerMessage.Property> messageProperties = new ArrayList<>();
        for (BcPropertyUpdate propertyUpdate : properties) {
            String propertyKey = propertyUpdate.getPropertyKey();
            String propertyName = propertyUpdate.getPropertyName();


            ElementOrPropertyStatus status = ElementOrPropertyStatus.getStatus(propertyUpdate);
            if (canHandle(element, propertyKey, propertyName, status)) {
                Long beforeDeleteTimestamp = propertyUpdate instanceof BcPropertyUpdateRemove
                        ? ((BcPropertyUpdateRemove) propertyUpdate).getBeforeDeleteTimestamp()
                        : null;
                DataWorkerMessage.Property property = new DataWorkerMessage.Property();
                property.setPropertyKey(propertyKey);
                property.setPropertyName(propertyName);
                property.setStatus(status);
                property.setBeforeActionTimestamp(beforeDeleteTimestamp);
                messageProperties.add(property);
            }
        }

        if (messageProperties.size() == 0) {
            return;
        }
        data.setProperties(messageProperties.toArray(new DataWorkerMessage.Property[messageProperties.size()]));

        addElementTypeToJson(data, element);

        if (workspaceId != null && !workspaceId.equals("")) {
            data.setWorkspaceId(workspaceId);
            data.setVisibilitySource(visibilitySource);
        }

        pushOnQueue(queueName, data.toBytes(), priority);
    }

    private void addElementTypeToJson(DataWorkerMessage data, Element element) {
        if (element instanceof Vertex) {
            data.setGraphVertexId(new String[]{element.getId()});
        } else if (element instanceof Edge) {
            data.setGraphEdgeId(new String[]{element.getId()});
        } else {
            throw new BcException("Unexpected element type: " + element.getClass().getName());
        }
    }

    public void pushElementImageQueue(
            Element element,
            String propertyKey,
            String propertyName,
            Priority priority
    ) {
        getGraph().flush();
        checkNotNull(element);
        JSONObject data = new JSONObject();
        if (element instanceof Vertex) {
            data.put("graphVertexId", element.getId());
        } else if (element instanceof Edge) {
            data.put("graphEdgeId", element.getId());
        } else {
            throw new BcException("Unexpected element type: " + element.getClass().getName());
        }
        data.put("propertyKey", propertyKey);
        data.put("propertyName", propertyName);
        pushOnQueue(queueName, data, priority);
    }

    public void pushMultipleGraphPropertyQueue(
            Iterable<? extends Element> elements,
            String propertyKey,
            String propertyName,
            String workspaceId,
            String visibilitySource,
            Priority priority,
            ElementOrPropertyStatus status,
            Long beforeActionTimestamp
    ) {
        checkNotNull(elements);
        if (!elements.iterator().hasNext()) {
            return;
        }

        getGraph().flush();

        DataWorkerMessage data = createPropertySpecificMessage(
                propertyKey,
                propertyName,
                workspaceId,
                visibilitySource,
                status,
                beforeActionTimestamp,
                priority
        );

        List<String> vertices = new ArrayList<>();
        List<String> edges = new ArrayList<>();
        for (Element element : elements) {
            if (!canHandle(element, propertyKey, propertyName, status)) {
                continue;
            }

            if (element instanceof Vertex) {
                vertices.add(element.getId());
            } else if (element instanceof Edge) {
                edges.add(element.getId());
            } else {
                throw new BcException("Unexpected element type: " + element.getClass().getName());
            }
        }

        data.setGraphVertexId(vertices.toArray(new String[vertices.size()]));
        data.setGraphEdgeId(edges.toArray(new String[edges.size()]));

        pushOnQueue(queueName, data.toBytes(), priority);
    }

    protected boolean canHandle(Element element, String propertyKey, String propertyName, ElementOrPropertyStatus status) {
        if (this.dataWorkerRunner == null) {
            return true;
        }
        if (propertyKey == null && propertyName == null) {
            return true;
        }

        return this.dataWorkerRunner.canHandle(element, propertyKey, propertyName, status);
    }


    private DataWorkerMessage createPropertySpecificMessage(
            String propertyKey,
            String propertyName,
            String workspaceId,
            String visibilitySource,
            ElementOrPropertyStatus status,
            Long beforeActionTimestamp,
            Priority priority
    ) {
        DataWorkerMessage data = new DataWorkerMessage();

        if (workspaceId != null && !workspaceId.equals("")) {
            data.setWorkspaceId(workspaceId);
            data.setVisibilitySource(visibilitySource);
        }
        data.setPropertyKey(propertyKey);
        data.setPropertyName(propertyName);
        data.setStatus(status);
        data.setPriority(priority);
        if (status == ElementOrPropertyStatus.DELETION || status == ElementOrPropertyStatus.HIDDEN) {
            checkNotNull(beforeActionTimestamp, "Timestamp before " + status + " cannot be null");
        }
        data.setBeforeActionTimestamp(beforeActionTimestamp);
        return data;
    }

//    public void pushLongRunningProcessQueue(JSONObject queueItem, Priority priority) {
//        pushOnQueue(workQueueNames.getLongRunningProcessQueueName(), queueItem, priority);
//    }

    public final void pushOnQueue(
            String queueName,
            JSONObject json,
            Priority priority
    ) {
        if (priority != null) {
            json.put("priority", priority.name());
        }
        pushOnQueue(queueName, json.toString().getBytes(), priority);
    }

    public abstract void pushOnQueue(
            String queueName,
            byte[] data,
            Priority priority
    );

    public abstract void flush();

    public void format() {
        deleteQueue(queueName);
    }

    protected abstract void deleteQueue(String queueName);

    public Graph getGraph() {
        return graph;
    }

    public abstract WorkerSpout createWorkerSpout(String queueName);

    public abstract Map<String, Status> getQueuesStatus();

    public void setDataWorkerRunner(DataWorkerRunner graphPropertyRunner) {
        this.dataWorkerRunner = graphPropertyRunner;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getQueueName() {
        return queueName;
    }

    protected Configuration getConfiguration() {
        return configuration;
    }

    public void pushLongRunningProcessQueue(JSONObject queueItem, Priority priority) {
        pushOnQueue(configuration.get(Configuration.LRP_INTERNAL_QUEUE_NAME, LRP_DEFAULT_INTERNAL_QUEUE_NAME), queueItem, priority);
    }}
