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

package io.bigconnect.biggraph.auth;

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.auth.SchemaDefine.Relationship;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.query.Condition;
import io.bigconnect.biggraph.backend.query.ConditionQuery;
import io.bigconnect.biggraph.backend.query.QueryResults;
import io.bigconnect.biggraph.backend.tx.GraphTransaction;
import io.bigconnect.biggraph.exception.NotFoundException;
import io.bigconnect.biggraph.iterator.MapperIterator;
import io.bigconnect.biggraph.schema.EdgeLabel;
import io.bigconnect.biggraph.schema.PropertyKey;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.structure.BigEdge;
import io.bigconnect.biggraph.structure.BigVertex;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.Directions;
import io.bigconnect.biggraph.type.define.BigKeys;
import io.bigconnect.biggraph.util.E;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class RelationshipManager<T extends Relationship> {

    private final BigGraphParams graph;
    private final String label;
    private final Function<Edge, T> deser;
    private final ThreadLocal<Boolean> autoCommit = new ThreadLocal<>();

    private static final long NO_LIMIT = -1L;

    public RelationshipManager(BigGraphParams graph, String label,
                               Function<Edge, T> deser) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.label = label;
        this.deser = deser;
        this.autoCommit.set(true);
    }

    private GraphTransaction tx() {
        return this.graph.systemTransaction();
    }

    private BigGraph graph() {
        return this.graph.graph();
    }

    private String unhideLabel() {
        return Hidden.unHide(this.label) ;
    }

    public Id add(T relationship) {
        E.checkArgumentNotNull(relationship, "Relationship can't be null");
        return this.save(relationship, false);
    }

    public Id update(T relationship) {
        E.checkArgumentNotNull(relationship, "Relationship can't be null");
        relationship.onUpdate();
        return this.save(relationship, true);
    }

    public T delete(Id id) {
        T relationship = null;
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            BigEdge edge = (BigEdge) edges.next();
            relationship = this.deser.apply(edge);
            this.tx().removeEdge(edge);
            this.commitOrRollback();
            assert !edges.hasNext();
        }
        return relationship;
    }

    public T get(Id id) {
        T relationship = null;
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            relationship = this.deser.apply(edges.next());
            assert !edges.hasNext();
        }
        if (relationship == null) {
            throw new NotFoundException("Can't find %s with id '%s'",
                                        this.unhideLabel(), id);
        }
        return relationship;
    }

    public boolean exists(Id id) {
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            Edge edge = edges.next();
            if (this.label.equals(edge.label())) {
                return true;
            }
        }
        return false;
    }

    public List<T> list(List<Id> ids) {
        return toList(this.queryById(ids));
    }

    public List<T> list(long limit) {
        Iterator<Edge> edges = this.queryRelationship(null, null, this.label,
                                                      ImmutableMap.of(), limit);
        return toList(edges);
    }

    public List<T> list(Id source, Directions direction,
                        String label, long limit) {
        Iterator<Edge> edges = this.queryRelationship(source, direction, label,
                                                      ImmutableMap.of(), limit);
        return toList(edges);
    }

    public List<T> list(Id source, Directions direction, String label,
                        String key, Object value, long limit) {
        Map<String, Object> conditions = ImmutableMap.of(key, value);
        Iterator<Edge> edges = this.queryRelationship(source, direction, label,
                                                      conditions, limit);
        return toList(edges);
    }

    protected List<T> toList(Iterator<Edge> edges) {
        Iterator<T> iter = new MapperIterator<>(edges, this.deser);
        // Convert iterator to list to avoid across thread tx accessed
        return (List<T>) QueryResults.toList(iter).list();
    }

    private Iterator<Edge> queryById(List<Id> ids) {
        Object[] idArray = ids.toArray(new Id[ids.size()]);
        return this.tx().queryEdges(idArray);
    }

    private Iterator<Edge> queryRelationship(Id source,
                                             Directions direction,
                                             String label,
                                             Map<String, Object> conditions,
                                             long limit) {
        ConditionQuery query = new ConditionQuery(BigType.EDGE);
        EdgeLabel el = this.graph().edgeLabel(label);
        if (direction == null) {
            direction = Directions.OUT;
        }
        if (source != null) {
            query.eq(BigKeys.OWNER_VERTEX, source);
            query.eq(BigKeys.DIRECTION, direction);
        }
        if (label != null) {
            query.eq(BigKeys.LABEL, el.id());
        }
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            PropertyKey pk = this.graph().propertyKey(entry.getKey());
            query.query(Condition.eq(pk.id(), entry.getValue()));
        }
        query.showHidden(true);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        Iterator<Edge> edges = this.tx().queryEdges(query);
        if (limit == NO_LIMIT) {
            return edges;
        }
        long[] size = new long[1];
        return new MapperIterator<>(edges, edge -> {
            if (++size[0] > limit) {
                return null;
            }
            return edge;
        });
    }

    private Id save(T relationship, boolean expectExists) {
        if (!this.graph().existsEdgeLabel(relationship.label())) {
            throw new BigGraphException("Schema is missing for %s '%s'",
                                    relationship.label(),
                                    relationship.source());
        }
        BigVertex source = this.newVertex(relationship.source(),
                                           relationship.sourceLabel());
        BigVertex target = this.newVertex(relationship.target(),
                                           relationship.targetLabel());
        BigEdge edge = source.constructEdge(relationship.label(), target,
                                             relationship.asArray());
        E.checkArgument(this.exists(edge.id()) == expectExists,
                        "Can't save %s '%s' that %s exists",
                        this.unhideLabel(), edge.id(),
                        expectExists ? "not" : "already");

        this.tx().addEdge(edge);
        this.commitOrRollback();
        return edge.id();
    }

    private BigVertex newVertex(Object id, String label) {
        VertexLabel vl = this.graph().vertexLabel(label);
        Id idValue = BigVertex.getIdValue(id);
        return BigVertex.create(this.tx(), idValue, vl);
    }

    private void commitOrRollback() {
        Boolean autoCommit = this.autoCommit.get();
        if (autoCommit != null && !autoCommit) {
            return;
        }
        this.tx().commitOrRollback();
    }

    public void autoCommit(boolean value) {
        autoCommit.set(value);
    }
}
