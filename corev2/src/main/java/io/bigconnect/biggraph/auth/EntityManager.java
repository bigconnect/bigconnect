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
import io.bigconnect.biggraph.auth.SchemaDefine.Entity;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.query.Condition;
import io.bigconnect.biggraph.backend.query.ConditionQuery;
import io.bigconnect.biggraph.backend.query.QueryResults;
import io.bigconnect.biggraph.backend.tx.GraphTransaction;
import io.bigconnect.biggraph.exception.NotFoundException;
import io.bigconnect.biggraph.iterator.MapperIterator;
import io.bigconnect.biggraph.schema.PropertyKey;
import io.bigconnect.biggraph.schema.VertexLabel;
import io.bigconnect.biggraph.structure.BigVertex;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.BigKeys;
import io.bigconnect.biggraph.util.E;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class EntityManager<T extends Entity> {

    private final BigGraphParams graph;
    private final String label;
    private final Function<Vertex, T> deser;
    private final ThreadLocal<Boolean> autoCommit = new ThreadLocal<>();

    private static final long NO_LIMIT = -1L;

    public EntityManager(BigGraphParams graph, String label,
                         Function<Vertex, T> deser) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.label = label;
        this.deser = deser;
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

    public Id add(T entity) {
        E.checkArgumentNotNull(entity, "Entity can't be null");
        return this.save(entity, false);
    }

    public Id update(T entity) {
        E.checkArgumentNotNull(entity, "Entity can't be null");
        entity.onUpdate();
        return this.save(entity, true);
    }

    public T delete(Id id) {
        T entity = null;
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            BigVertex vertex = (BigVertex) vertices.next();
            entity = this.deser.apply(vertex);
            this.tx().removeVertex(vertex);
            this.commitOrRollback();
            assert !vertices.hasNext();
        }
        return entity;
    }

    public T get(Id id) {
        T entity = null;
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            entity = this.deser.apply(vertices.next());
            assert !vertices.hasNext();
        }
        if (entity == null) {
            throw new NotFoundException("Can't find %s with id '%s'",
                                        this.unhideLabel(), id);
        }
        return entity;
    }

    public boolean exists(Id id) {
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            Vertex vertex = vertices.next();
            if (this.label.equals(vertex.label())) {
                return true;
            }
        }
        return false;
    }

    public List<T> list(List<Id> ids) {
        return toList(this.queryById(ids));
    }

    public List<T> list(long limit) {
        return toList(this.queryEntity(this.label, ImmutableMap.of(), limit));
    }

    protected List<T> query(String key, Object value, long limit) {
        Map<String, Object> conditions = ImmutableMap.of(key, value);
        return toList(this.queryEntity(this.label, conditions, limit));
    }

    protected List<T> toList(Iterator<Vertex> vertices) {
        Iterator<T> iter = new MapperIterator<>(vertices, this.deser);
        // Convert iterator to list to avoid across thread tx accessed
        return (List<T>) QueryResults.toList(iter).list();
    }

    private Iterator<Vertex> queryById(List<Id> ids) {
        Object[] idArray = ids.toArray(new Id[ids.size()]);
        return this.tx().queryVertices(idArray);
    }

    private Iterator<Vertex> queryEntity(String label,
                                         Map<String, Object> conditions,
                                         long limit) {
        ConditionQuery query = new ConditionQuery(BigType.VERTEX);
        VertexLabel vl = this.graph().vertexLabel(label);
        query.eq(BigKeys.LABEL, vl.id());
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            PropertyKey pkey = this.graph().propertyKey(entry.getKey());
            query.query(Condition.eq(pkey.id(), entry.getValue()));
        }
        query.showHidden(true);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        return this.tx().queryVertices(query);
    }

    private Id save(T entity, boolean expectExists) {
        // Construct vertex from task
        BigVertex vertex = this.constructVertex(entity);
        E.checkArgument(this.exists(vertex.id()) == expectExists,
                        "Can't save %s '%s' that %s exists",
                        this.unhideLabel(), vertex.id(),
                        expectExists ? "not" : "already");

        // Add or update user in backend store, stale index might exist
        vertex = this.tx().addVertex(vertex);
        this.commitOrRollback();
        return vertex.id();
    }

    private BigVertex constructVertex(Entity entity) {
        if (!this.graph().existsVertexLabel(entity.label())) {
            throw new BigGraphException("Schema is missing for %s '%s'",
                                    entity.label(), entity.id());
        }
        return this.tx().constructVertex(false, entity.asArray());
    }

    private void commitOrRollback() {
        Boolean autoCommit = this.autoCommit.get();
        if (autoCommit != null && !autoCommit) {
            return;
        }
        this.tx().commitOrRollback();
    }

    public void autoCommit(boolean value) {
        this.autoCommit.set(value);
    }
}
