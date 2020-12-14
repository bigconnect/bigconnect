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
import com.google.inject.Singleton;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.graph.GraphUpdateContext;
import com.mware.core.model.properties.LongRunningProcessSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.ge.*;
import com.mware.ge.util.ConvertingIterable;
import org.json.JSONObject;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.ge.util.IterableUtils.toList;

@Singleton
public class GeLongRunningProcessRepository extends LongRunningProcessRepository {
    private final WorkQueueRepository workQueueRepository;
    private final WebQueueRepository webQueueRepository;
    private final GraphRepository graphRepository;
    private final UserRepository userRepository;
    private final Graph graph;
    private final AuthorizationRepository authorizationRepository;

    @Inject
    public GeLongRunningProcessRepository(
            GraphRepository graphRepository,
            GraphAuthorizationRepository graphAuthorizationRepository,
            UserRepository userRepository,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            Graph graph,
            AuthorizationRepository authorizationRepository
    ) {
        this.graphRepository = graphRepository;
        this.userRepository = userRepository;
        this.workQueueRepository = workQueueRepository;
        this.webQueueRepository = webQueueRepository;
        this.graph = graph;
        this.authorizationRepository = authorizationRepository;

        graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);
    }

    @Override
    public String enqueue(JSONObject longRunningProcessQueueItem, User user, Authorizations authorizations) {
        authorizations = getAuthorizations(user);

        Vertex userVertex;
        if (user instanceof SystemUser) {
            userVertex = null;
        } else {
            userVertex = graph.getVertex(user.getUserId(), authorizations);
            checkNotNull(userVertex, "Could not find user with id: " + user.getUserId());
        }
        Visibility visibility = getVisibility();

        String longRunningProcessVertexId;
        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.LOW, user, authorizations)) {
            ctx.setPushOnQueue(false);
            longRunningProcessVertexId = ctx.update(this.graph.prepareVertex(visibility, LongRunningProcessSchema.LONG_RUNNING_PROCESS_CONCEPT_NAME), elemCtx -> {
                PropertyMetadata metadata = new PropertyMetadata(user, new VisibilityJson(), visibility);
                elemCtx.updateBuiltInProperties(metadata);
                longRunningProcessQueueItem.put("enqueueTime", System.currentTimeMillis());
                longRunningProcessQueueItem.put("userId", user.getUserId());
                LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.updateProperty(elemCtx, longRunningProcessQueueItem, metadata);
            }).get().getId();

            if (userVertex != null) {
                ctx.getOrCreateEdgeAndUpdate(
                        null,
                        userVertex.getId(),
                        longRunningProcessVertexId,
                        LongRunningProcessSchema.LONG_RUNNING_PROCESS_TO_USER_EDGE_NAME,
                        visibility,
                        elemCtx -> {
                        }
                );
            }
        } catch (Exception ex) {
            throw new BcException("Could not create long running process vertex", ex);
        }

        longRunningProcessQueueItem.put("id", longRunningProcessVertexId);
        webQueueRepository.broadcastLongRunningProcessChange(longRunningProcessQueueItem);
        workQueueRepository.pushLongRunningProcessQueue(longRunningProcessQueueItem, Priority.NORMAL);

        return longRunningProcessVertexId;
    }

    public Authorizations getAuthorizations(User user) {
        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING
        );
        return authorizations;
    }

    @Override
    public void beginWork(JSONObject longRunningProcessQueueItem) {
        super.beginWork(longRunningProcessQueueItem);
        updateVertexWithJson(longRunningProcessQueueItem);
    }

    @Override
    public void ack(JSONObject longRunningProcessQueueItem) {
        updateVertexWithJson(longRunningProcessQueueItem);
    }

    @Override
    public void nak(JSONObject longRunningProcessQueueItem, Throwable ex) {
        updateVertexWithJson(longRunningProcessQueueItem);
    }

    public void updateVertexWithJson(JSONObject longRunningProcessQueueItem) {
        String longRunningProcessGraphVertexId = longRunningProcessQueueItem.getString("id");
        Authorizations authorizations = getAuthorizations(userRepository.getSystemUser());
        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.LOW, userRepository.getSystemUser(), authorizations)) {
            ctx.setPushOnQueue(false);
            Vertex vertex = this.graph.getVertex(longRunningProcessGraphVertexId, authorizations);
            checkNotNull(vertex, "Could not find long running process vertex: " + longRunningProcessGraphVertexId);
            ctx.update(vertex, elemCtx -> {
                PropertyMetadata metadata = new PropertyMetadata(userRepository.getSystemUser(), new VisibilityJson(), getVisibility());
                LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.updateProperty(
                        elemCtx,
                        longRunningProcessQueueItem,
                        metadata
                );
            });
        }
    }

    @Override
    public List<JSONObject> getLongRunningProcesses(User user) {
        Authorizations authorizations = getAuthorizations(user);
        Vertex userVertex = graph.getVertex(user.getUserId(), authorizations);
        checkNotNull(userVertex, "Could not find user with id: " + user.getUserId());
        Iterable<Vertex> longRunningProcessVertices = userVertex.getVertices(
                Direction.OUT,
                LongRunningProcessSchema.LONG_RUNNING_PROCESS_TO_USER_EDGE_NAME,
                authorizations
        );
        return toList(new ConvertingIterable<Vertex, JSONObject>(longRunningProcessVertices) {
            @Override
            protected JSONObject convert(Vertex longRunningProcessVertex) {
                JSONObject json = LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.getPropertyValue(
                        longRunningProcessVertex);
                json.put("id", longRunningProcessVertex.getId());
                return json;
            }
        });
    }

    @Override
    public JSONObject findById(String longRunningProcessId, User user) {
        Authorizations authorizations = getAuthorizations(user);
        Vertex vertex = this.graph.getVertex(longRunningProcessId, authorizations);
        if (vertex == null) {
            return null;
        }
        return LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.getPropertyValue(vertex);
    }

    @Override
    public void cancel(String longRunningProcessId, User user) {
        // TODO: this only removes the UI job. There is no support for actually cancelling the LongRunningProcess
        Authorizations authorizations = getAuthorizations(userRepository.getSystemUser());
        Vertex vertex = this.graph.getVertex(longRunningProcessId, authorizations);
        checkNotNull(vertex, "Could not find long running process vertex: " + longRunningProcessId);
        JSONObject json = LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.getPropertyValue(vertex);
        json.put("canceled", true);
        json.put("id", longRunningProcessId);
        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.LOW, user, authorizations)) {
            ctx.setPushOnQueue(false);
            VertexBuilder vb = graph.prepareVertex(longRunningProcessId, vertex.getVisibility(), LongRunningProcessSchema.LONG_RUNNING_PROCESS_CONCEPT_NAME);
            ctx.update(vb, elemCtx -> {
                PropertyMetadata metadata = new PropertyMetadata(userRepository.getSystemUser(), new VisibilityJson(), getVisibility());
                LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.updateProperty(
                        elemCtx,
                        json,
                        metadata
                );
            });
        }

        webQueueRepository.broadcastLongRunningProcessChange(json);
    }

    @Override
    public void reportProgress(String longRunningProcessGraphVertexId, double progressPercent, String message) {
        Authorizations authorizations = getAuthorizations(userRepository.getSystemUser());
        Vertex vertex = this.graph.getVertex(longRunningProcessGraphVertexId, authorizations);
        checkNotNull(vertex, "Could not find long running process vertex: " + longRunningProcessGraphVertexId);

        JSONObject json = LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.getPropertyValue(vertex);
        if (json.optBoolean("canceled", false)) {
            throw new BcException("Unable to update progress of cancelled process");
        }

        json.put("progress", progressPercent);
        json.put("progressMessage", message);
        json.put("id", longRunningProcessGraphVertexId);

        LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.setProperty(
                vertex,
                json,
                getVisibility(),
                authorizations
        );

        this.graph.flush();
        webQueueRepository.broadcastLongRunningProcessChange(json);
    }

    @Override
    public void delete(String longRunningProcessId, User authUser) {
        Authorizations authorizations = getAuthorizations(authUser);
        Vertex vertex = this.graph.getVertex(longRunningProcessId, authorizations);
        JSONObject json = null;
        if (vertex != null) {
            json = LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.getPropertyValue(vertex);
        }
        this.graph.deleteVertex(vertex, authorizations);
        this.graph.flush();

        if (json != null) {
            webQueueRepository.broadcastLongRunningProcessDeleted(json);
        }
    }

    private Visibility getVisibility() {
        return new BcVisibility(VISIBILITY_STRING).getVisibility();
    }
}
