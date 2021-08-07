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
package com.mware.core.ping;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.query.*;
import com.mware.ge.query.aggregations.StatisticsAggregation;
import com.mware.ge.query.aggregations.StatisticsResult;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.values.storable.Values;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.util.List;

@Singleton
public class PingUtil {
    public static final String VISIBILITY_STRING = "ping";
    public static final Visibility VISIBILITY = new BcVisibility(VISIBILITY_STRING).getVisibility();
    private final User systemUser;
    private final VisibilityTranslator visibilityTranslator;

    @Inject
    public PingUtil(
            GraphAuthorizationRepository graphAuthorizationRepository,
            UserRepository userRepository,
            VisibilityTranslator visibilityTranslator
    ) {
        graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);
        this.systemUser = userRepository.getSystemUser();
        this.visibilityTranslator = visibilityTranslator;
    }

    public String search(Graph graph, Authorizations authorizations) {
        Query query = graph.query(GeQueryBuilders.searchAll().limit(1), authorizations);
        List<Vertex> vertices = Lists.newArrayList(query.vertices());
        if (vertices.size() == 0) {
            throw new BcException("query returned no vertices");
        } else if (vertices.size() > 1) {
            throw new BcException("query returned more than one vertex");
        }
        return vertices.get(0).getId();
    }

    public void retrieve(String vertexId, Graph graph, Authorizations authorizations) {
        Vertex retrievedVertex = graph.getVertex(vertexId, authorizations);
        if (retrievedVertex == null) {
            throw new BcException("failed to retrieve vertex by id: " + vertexId);
        }
    }

    public Vertex createVertex(String remoteAddr, long searchTime, long retrievalTime, Graph graph, Authorizations authorizations) {
        ZonedDateTime createDate = ZonedDateTime.now();
        String vertexId = PingSchema.getVertexId(createDate);
        ElementMutation<Vertex> mutation = graph.prepareVertex(vertexId, VISIBILITY, PingSchema.CONCEPT_NAME_PING);
        PingSchema.CREATE_DATE.setProperty(mutation, createDate, VISIBILITY);
        PingSchema.CREATE_REMOTE_ADDR.setProperty(mutation, remoteAddr, VISIBILITY);
        PingSchema.SEARCH_TIME_MS.setProperty(mutation, searchTime, VISIBILITY);
        PingSchema.RETRIEVAL_TIME_MS.setProperty(mutation, retrievalTime, VISIBILITY);
        Vertex vertex = mutation.save(authorizations);
        graph.flush();
        return vertex;
    }

    public void enqueueToWorkQueue(Vertex vertex, WorkQueueRepository workQueueRepository, WebQueueRepository webQueueRepository, Priority priority) {
        if(webQueueRepository.shouldBroadcast(priority)) {
            webQueueRepository.broadcastPropertyChange(vertex, null, null, null);
        }
        workQueueRepository.pushGraphPropertyQueue(
                vertex,
                null,
                null,
                null,
                null,
                priority,
                ElementOrPropertyStatus.UPDATE,
                null
        );
    }

    public void gpwUpdate(Vertex vertex, Graph graph, Authorizations authorizations) {
        ZonedDateTime updateDate = ZonedDateTime.now();
        Long waitTimeMs = updateDate.toInstant().toEpochMilli() - PingSchema.CREATE_DATE.getPropertyValueRequired(vertex).toInstant().toEpochMilli();
        ElementMutation<Vertex> mutation = vertex.prepareMutation();
        PingSchema.GRAPH_PROPERTY_WORKER_DATE.setProperty(mutation, updateDate, VISIBILITY);
        PingSchema.GRAPH_PROPERTY_WORKER_HOSTNAME.setProperty(mutation, getHostname(), VISIBILITY);
        PingSchema.GRAPH_PROPERTY_WORKER_HOST_ADDRESS.setProperty(mutation, getHostAddress(), VISIBILITY);
        PingSchema.GRAPH_PROPERTY_WORKER_WAIT_TIME_MS.setProperty(mutation, waitTimeMs, VISIBILITY);
        mutation.save(authorizations);
        graph.flush();
    }

    public void enqueueToLongRunningProcess(Vertex vertex, LongRunningProcessRepository longRunningProcessRepository, Authorizations authorizations) {
        longRunningProcessRepository.enqueue(new PingLongRunningProcessQueueItem(vertex).toJson(), systemUser, authorizations);
    }

    public void lrpUpdate(Vertex vertex, Graph graph, Authorizations authorizations) {
        ZonedDateTime updateDate = ZonedDateTime.now();
        Long waitTimeMs = updateDate.toInstant().toEpochMilli() - PingSchema.CREATE_DATE.getPropertyValueRequired(vertex).toInstant().toEpochMilli();
        ElementMutation<Vertex> mutation = vertex.prepareMutation();
        PingSchema.LONG_RUNNING_PROCESS_DATE.setProperty(mutation, updateDate, VISIBILITY);
        PingSchema.LONG_RUNNING_PROCESS_HOSTNAME.setProperty(mutation, getHostname(), VISIBILITY);
        PingSchema.LONG_RUNNING_PROCESS_HOST_ADDRESS.setProperty(mutation, getHostAddress(), VISIBILITY);
        PingSchema.LONG_RUNNING_PROCESS_WAIT_TIME_MS.setProperty(mutation, waitTimeMs, VISIBILITY);
        mutation.save(authorizations);
        graph.flush();
    }

    public JSONObject getAverages(int minutes, Graph graph, Authorizations authorizations) {
        ZonedDateTime minutesAgo = ZonedDateTime.now().minusMinutes(60);
        GeQueryBuilder queryBuilder = GeQueryBuilders.boolQuery()
                .and(GeQueryBuilders.hasConceptType(PingSchema.CONCEPT_NAME_PING))
                .and(GeQueryBuilders.hasFilter(PingSchema.CREATE_DATE.getPropertyName(), Compare.GREATER_THAN, Values.temporalValue(minutesAgo)))
                .limit(0);
        Query q = graph.query(queryBuilder, authorizations);
        q.addAggregation(new StatisticsAggregation(PingSchema.SEARCH_TIME_MS.getPropertyName(), PingSchema.SEARCH_TIME_MS.getPropertyName()));
        q.addAggregation(new StatisticsAggregation(PingSchema.RETRIEVAL_TIME_MS.getPropertyName(), PingSchema.RETRIEVAL_TIME_MS.getPropertyName()));
        q.addAggregation(new StatisticsAggregation(PingSchema.GRAPH_PROPERTY_WORKER_WAIT_TIME_MS.getPropertyName(), PingSchema.GRAPH_PROPERTY_WORKER_WAIT_TIME_MS.getPropertyName()));
        q.addAggregation(new StatisticsAggregation(PingSchema.LONG_RUNNING_PROCESS_WAIT_TIME_MS.getPropertyName(), PingSchema.LONG_RUNNING_PROCESS_WAIT_TIME_MS.getPropertyName()));
        QueryResultsIterable<Vertex> vertices = q.vertices();
        StatisticsResult searchTimeAgg = vertices.getAggregationResult(PingSchema.SEARCH_TIME_MS.getPropertyName(), StatisticsResult.class);
        StatisticsResult retrievalTimeAgg = vertices.getAggregationResult(PingSchema.RETRIEVAL_TIME_MS.getPropertyName(), StatisticsResult.class);
        StatisticsResult gpwWaitTimeAgg = vertices.getAggregationResult(PingSchema.GRAPH_PROPERTY_WORKER_WAIT_TIME_MS.getPropertyName(), StatisticsResult.class);
        StatisticsResult lrpWaitTimeAgg = vertices.getAggregationResult(PingSchema.LONG_RUNNING_PROCESS_WAIT_TIME_MS.getPropertyName(), StatisticsResult.class);

        JSONObject json = new JSONObject();
        json.put("pingCount", searchTimeAgg.getCount());
        json.put("averageSearchTime", searchTimeAgg.getAverage());
        json.put("averageRetrievalTime", retrievalTimeAgg.getAverage());
        json.put("graphPropertyWorkerCount", gpwWaitTimeAgg.getCount());
        json.put("averageGraphPropertyWorkerWaitTime", gpwWaitTimeAgg.getAverage());
        json.put("longRunningProcessCount", lrpWaitTimeAgg.getCount());
        json.put("averageLongRunningProcessWaitTime", lrpWaitTimeAgg.getAverage());
        return json;
    }

    private String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            // do nothing
        }
        return "";
    }

    private String getHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            // do nothing
        }
        return "";
    }
}
