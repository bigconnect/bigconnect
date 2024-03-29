/*
 * Copyright 2021 BigConnect Authors
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.bigconnect.biggraph.traversal.optimize;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.query.ConditionQuery;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.query.QueryResults;
import io.bigconnect.biggraph.backend.tx.GraphTransaction;
import io.bigconnect.biggraph.type.define.Directions;
import io.bigconnect.biggraph.util.Log;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import java.util.*;

public final class BigVertexStep<E extends Element>
             extends VertexStep<E> implements QueryHolder {

    private static final long serialVersionUID = -7850636388424382454L;

    private static final Logger LOG = Log.logger(BigVertexStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();

    // Store limit/order-by
    private final Query queryInfo = new Query(null);

    private Iterator<E> lastTimeResults = QueryResults.emptyIterator();

    public BigVertexStep(final VertexStep<E> originVertexStep) {
        super(originVertexStep.getTraversal(),
              originVertexStep.getReturnClass(),
              originVertexStep.getDirection(),
              originVertexStep.getEdgeLabels());
        originVertexStep.getLabels().forEach(this::addLabel);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        Iterator<E> results;
        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        if (queryVertex) {
            results = (Iterator<E>) this.vertices(traverser);
        } else {
            assert queryEdge;
            results = (Iterator<E>) this.edges(traverser);
        }
        this.lastTimeResults = results;
        return results;
    }

    private Iterator<Vertex> vertices(Traverser.Admin<Vertex> traverser) {
        BigGraph graph = TraversalUtil.getGraph(this);
        Vertex vertex = traverser.get();

        Iterator<Edge> edges = this.edges(traverser);
        Iterator<Vertex> vertices = graph.adjacentVertices(edges);

        if (LOG.isDebugEnabled()) {
            LOG.debug("HugeVertexStep.vertices(): is there adjacent " +
                      "vertices of {}: {}, has={}",
                      vertex.id(), vertices.hasNext(), this.hasContainers);
        }

        if (this.hasContainers.isEmpty()) {
            return vertices;
        }

        // TODO: query by vertex index to optimize
        return TraversalUtil.filterResult(this.hasContainers, vertices);
    }

    private Iterator<Edge> edges(Traverser.Admin<Vertex> traverser) {
        BigGraph graph = TraversalUtil.getGraph(this);
        List<HasContainer> conditions = this.hasContainers;

        // Query for edge with conditions(else conditions for vertex)
        boolean withEdgeCond = this.returnsEdge() && !conditions.isEmpty();
        boolean withVertexCond = this.returnsVertex() && !conditions.isEmpty();

        Id vertex = (Id) traverser.get().id();
        Directions direction = Directions.convert(this.getDirection());
        Id[] edgeLabels = graph.mapElName2Id(this.getEdgeLabels());

        LOG.debug("HugeVertexStep.edges(): vertex={}, direction={}, " +
                  "edgeLabels={}, has={}",
                  vertex, direction, edgeLabels, this.hasContainers);

        ConditionQuery query = GraphTransaction.constructEdgesQuery(
                               vertex, direction, edgeLabels);
        // Query by sort-keys
        if (withEdgeCond && edgeLabels.length > 0) {
            TraversalUtil.fillConditionQuery(query, conditions, graph);
            if (!GraphTransaction.matchPartialEdgeSortKeys(query, graph)) {
                // Can't query by sysprop and by index (HugeGraph-749)
                query.resetUserpropConditions();
            } else if (GraphTransaction.matchFullEdgeSortKeys(query, graph)) {
                // All sysprop conditions are in sort-keys
                withEdgeCond = false;
            } else {
                // Partial sysprop conditions are in sort-keys
                assert query.userpropKeys().size() > 0;
            }
        }

        // Query by has(id)
        if (!query.ids().isEmpty()) {
            // Ignore conditions if query by edge id in has-containers
            // FIXME: should check that the edge id matches the `vertex`
            query.resetConditions();
            LOG.warn("It's not recommended to query by has(id)");
        }

        /*
         * Unset limit when needed to filter property after store query
         * like query: outE().has(k,v).limit(n)
         * NOTE: outE().limit(m).has(k,v).limit(n) will also be unset limit,
         * Can't unset limit if query by paging due to page position will be
         * exceeded when reaching the limit in tinkerpop layer
         */
        if (withEdgeCond || withVertexCond) {
            io.bigconnect.biggraph.util.E.checkArgument(!this.queryInfo().paging(),
                                                     "Can't query by paging " +
                                                     "and filtering");
            this.queryInfo().limit(Query.NO_LIMIT);
        }

        query = this.injectQueryInfo(query);

        // Do query
        Iterator<Edge> edges = graph.edges(query);

        // Do filter by edge conditions
        if (withEdgeCond) {
            return TraversalUtil.filterResult(conditions, edges);
        }
        return edges;
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        }

        return StringFactory.stepString(
               this,
               getDirection(),
               Arrays.asList(getEdgeLabels()),
               getReturnClass().getSimpleName(),
               this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer has) {
        if (SYSPROP_PAGE.equals(has.getKey())) {
            this.setPage((String) has.getValue());
            return;
        }
        this.hasContainers.add(has);
    }

    @Override
    public Query queryInfo() {
        return this.queryInfo;
    }

    @Override
    public Iterator<?> lastTimeResults() {
        return this.lastTimeResults;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^
               this.queryInfo.hashCode() ^
               this.hasContainers.hashCode();
    }
}
