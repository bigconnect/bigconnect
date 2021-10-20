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

package io.bigconnect.biggraph.backend.tx;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.backend.BackendException;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.backend.query.ConditionQuery;
import io.bigconnect.biggraph.backend.query.Query;
import io.bigconnect.biggraph.backend.query.QueryResults;
import io.bigconnect.biggraph.backend.store.BackendEntry;
import io.bigconnect.biggraph.backend.store.BackendStore;
import io.bigconnect.biggraph.config.CoreOptions;
import io.bigconnect.biggraph.exception.NotAllowException;
import io.bigconnect.biggraph.job.JobBuilder;
import io.bigconnect.biggraph.job.schema.*;
import io.bigconnect.biggraph.perf.PerfUtil.Watched;
import io.bigconnect.biggraph.schema.*;
import io.bigconnect.biggraph.task.BigTask;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.type.define.GraphMode;
import io.bigconnect.biggraph.type.define.BigKeys;
import io.bigconnect.biggraph.type.define.SchemaStatus;
import io.bigconnect.biggraph.type.define.WriteType;
import io.bigconnect.biggraph.util.DateUtil;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.LockUtil;
import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class SchemaTransaction extends IndexableTransaction {

    private SchemaIndexTransaction indexTx;

    public SchemaTransaction(BigGraphParams graph, BackendStore store) {
        super(graph, store);
        this.autoCommit(true);

        this.indexTx = new SchemaIndexTransaction(graph, store);
    }

    @Override
    protected AbstractTransaction indexTransaction() {
        return this.indexTx;
    }

    @Override
    protected void beforeRead() {
        /*
         * NOTE: each schema operation will be auto committed,
         * we expect the tx is clean when query.
         */
        if (this.hasUpdate()) {
            throw new BackendException("There are still dirty changes");
        }
    }

    @Watched(prefix = "schema")
    public List<PropertyKey> getPropertyKeys() {
        return this.getAllSchema(BigType.PROPERTY_KEY);
    }

    @Watched(prefix = "schema")
    public List<VertexLabel> getVertexLabels() {
        return this.getAllSchema(BigType.VERTEX_LABEL);
    }

    @Watched(prefix = "schema")
    public List<EdgeLabel> getEdgeLabels() {
        return this.getAllSchema(BigType.EDGE_LABEL);
    }

    @Watched(prefix = "schema")
    public List<IndexLabel> getIndexLabels() {
        return this.getAllSchema(BigType.INDEX_LABEL);
    }

    @Watched(prefix = "schema")
    public Id addPropertyKey(PropertyKey propertyKey) {
        this.addSchema(propertyKey);
        if (propertyKey.olap()) {
            return this.createOlapPk(propertyKey);
        }
        return IdGenerator.ZERO;
    }

    @Watched(prefix = "schema")
    public PropertyKey getPropertyKey(Id id) {
        E.checkArgumentNotNull(id, "Property key id can't be null");
        return this.getSchema(BigType.PROPERTY_KEY, id);
    }

    @Watched(prefix = "schema")
    public PropertyKey getPropertyKey(String name) {
        E.checkArgumentNotNull(name, "Property key name can't be null");
        E.checkArgument(!name.isEmpty(), "Property key name can't be empty");
        return this.getSchema(BigType.PROPERTY_KEY, name);
    }

    @Watched(prefix = "schema")
    public Id removePropertyKey(Id id) {
        LOG.debug("SchemaTransaction remove property key '{}'", id);
        PropertyKey propertyKey = this.getPropertyKey(id);
        // If the property key does not exist, return directly
        if (propertyKey == null) {
            return null;
        }

        List<VertexLabel> vertexLabels = this.getVertexLabels();
        for (VertexLabel vertexLabel : vertexLabels) {
            if (vertexLabel.properties().contains(id)) {
                throw new NotAllowException(
                          "Not allowed to remove property key: '%s' " +
                          "because the vertex label '%s' is still using it.",
                          propertyKey, vertexLabel.name());
            }
        }

        List<EdgeLabel> edgeLabels = this.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.properties().contains(id)) {
                throw new NotAllowException(
                          "Not allowed to remove property key: '%s' " +
                          "because the edge label '%s' is still using it.",
                          propertyKey, edgeLabel.name());
            }
        }

        if (propertyKey.oltp()) {
            this.removeSchema(propertyKey);
            return IdGenerator.ZERO;
        } else {
            return this.removeOlapPk(propertyKey);
        }
    }

    @Watched(prefix = "schema")
    public void addVertexLabel(VertexLabel vertexLabel) {
        this.addSchema(vertexLabel);
    }

    @Watched(prefix = "schema")
    public VertexLabel getVertexLabel(Id id) {
        E.checkArgumentNotNull(id, "Vertex label id can't be null");
        if (SchemaElement.OLAP_ID.equals(id)) {
            return VertexLabel.OLAP_VL;
        }
        return this.getSchema(BigType.VERTEX_LABEL, id);
    }

    @Watched(prefix = "schema")
    public VertexLabel getVertexLabel(String name) {
        E.checkArgumentNotNull(name, "Vertex label name can't be null");
        E.checkArgument(!name.isEmpty(), "Vertex label name can't be empty");
        if (SchemaElement.OLAP.equals(name)) {
            return VertexLabel.OLAP_VL;
        }
        return this.getSchema(BigType.VERTEX_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeVertexLabel(Id id) {
        LOG.debug("SchemaTransaction remove vertex label '{}'", id);
        SchemaCallable callable = new VertexLabelRemoveCallable();
        VertexLabel schema = this.getVertexLabel(id);
        return asyncRun(this.graph(), schema, callable);
    }

    @Watched(prefix = "schema")
    public void addEdgeLabel(EdgeLabel edgeLabel) {
        this.addSchema(edgeLabel);
    }

    @Watched(prefix = "schema")
    public EdgeLabel getEdgeLabel(Id id) {
        E.checkArgumentNotNull(id, "Edge label id can't be null");
        return this.getSchema(BigType.EDGE_LABEL, id);
    }

    @Watched(prefix = "schema")
    public EdgeLabel getEdgeLabel(String name) {
        E.checkArgumentNotNull(name, "Edge label name can't be null");
        E.checkArgument(!name.isEmpty(), "Edge label name can't be empty");
        return this.getSchema(BigType.EDGE_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeEdgeLabel(Id id) {
        LOG.debug("SchemaTransaction remove edge label '{}'", id);
        SchemaCallable callable = new EdgeLabelRemoveCallable();
        EdgeLabel schema = this.getEdgeLabel(id);
        return asyncRun(this.graph(), schema, callable);
    }

    @Watched(prefix = "schema")
    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel) {
        this.addSchema(indexLabel);

        /*
         * Update index name in base-label(VL/EL)
         * TODO: should wrap update base-label and create index in one tx.
         */
        if (schemaLabel instanceof VertexLabel &&
            schemaLabel.equals(VertexLabel.OLAP_VL)) {
            return;
        }
        schemaLabel.indexLabel(indexLabel.id());
        this.updateSchema(schemaLabel);
    }

    @Watched(prefix = "schema")
    public IndexLabel getIndexLabel(Id id) {
        E.checkArgumentNotNull(id, "Index label id can't be null");
        return this.getSchema(BigType.INDEX_LABEL, id);
    }

    @Watched(prefix = "schema")
    public IndexLabel getIndexLabel(String name) {
        E.checkArgumentNotNull(name, "Index label name can't be null");
        E.checkArgument(!name.isEmpty(), "Index label name can't be empty");
        return this.getSchema(BigType.INDEX_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeIndexLabel(Id id) {
        LOG.debug("SchemaTransaction remove index label '{}'", id);
        SchemaCallable callable = new IndexLabelRemoveCallable();
        IndexLabel schema = this.getIndexLabel(id);
        return asyncRun(this.graph(), schema, callable);
    }

    @Watched(prefix = "schema")
    public Id rebuildIndex(SchemaElement schema) {
        return this.rebuildIndex(schema, ImmutableSet.of());
    }

    @Watched(prefix = "schema")
    public Id rebuildIndex(SchemaElement schema, Set<Id> dependencies) {
        LOG.debug("SchemaTransaction rebuild index for {} with id '{}'",
                  schema.type(), schema.id());
        SchemaCallable callable = new RebuildIndexCallable();
        return asyncRun(this.graph(), schema, callable, dependencies);
    }

    public void createIndexLabelForOlapPk(PropertyKey propertyKey) {
        WriteType writeType = propertyKey.writeType();
        if (writeType == WriteType.OLTP ||
            writeType == WriteType.OLAP_COMMON) {
            return;
        }

        String indexName = SchemaElement.OLAP + "_by_" + propertyKey.name();
        IndexLabel.Builder builder = this.graph().schema()
                                         .indexLabel(indexName)
                                         .onV(SchemaElement.OLAP)
                                         .by(propertyKey.name());
        if (propertyKey.writeType() == WriteType.OLAP_SECONDARY) {
            builder.secondary();
        } else {
            assert propertyKey.writeType() == WriteType.OLAP_RANGE;
            builder.range();
        }
        builder.build();
        this.graph().addIndexLabel(VertexLabel.OLAP_VL, builder.build());
    }

    public Id createOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction create olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaCallable callable = new OlapPropertyKeyCreateCallable();
        return asyncRun(this.graph(), propertyKey, callable);
    }

    public Id clearOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction clear olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaCallable callable = new OlapPropertyKeyClearCallable();
        return asyncRun(this.graph(), propertyKey, callable);
    }

    public Id removeOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction remove olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaCallable callable = new OlapPropertyKeyRemoveCallable();
        return asyncRun(this.graph(), propertyKey, callable);
    }

    public void createOlapPk(Id id) {
        this.store().provider().createOlapTable(this.graph(), id);
    }

    public void initAndRegisterOlapTables() {
        for (PropertyKey pk : this.getPropertyKeys()) {
            if (pk.olap()) {
                this.store().provider().initAndRegisterOlapTable(this.graph(),
                                                                 pk.id());
            }
        }
    }

    public void clearOlapPk(Id id) {
        this.store().provider().clearOlapTable(this.graph(), id);
    }

    public void removeOlapPk(Id id) {
        this.store().provider().removeOlapTable(this.graph(), id);
    }

    @Watched(prefix = "schema")
    public void updateSchemaStatus(SchemaElement schema, SchemaStatus status) {
        schema.status(status);
        this.updateSchema(schema);
    }

    @Watched(prefix = "schema")
    public boolean existsSchemaId(BigType type, Id id) {
        return this.getSchema(type, id) != null;
    }

    protected void updateSchema(SchemaElement schema) {
        this.addSchema(schema);
    }

    protected void addSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction add {} with id '{}'",
                  schema.type(), schema.id());
        setCreateTimeIfNeeded(schema);

        LockUtil.Locks locks = new LockUtil.Locks(this.params().name());
        try {
            locks.lockWrites(LockUtil.hugeType2Group(schema.type()),
                             schema.id());
            this.beforeWrite();
            this.doInsert(this.serialize(schema));
            this.indexTx.updateNameIndex(schema, false);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
    }

    protected <T extends SchemaElement> T getSchema(BigType type, Id id) {
        LOG.debug("SchemaTransaction get {} by id '{}'",
                  type.readableName(), id);
        this.beforeRead();
        BackendEntry entry = this.query(type, id);
        if (entry == null) {
            return null;
        }
        T schema = this.deserialize(entry, type);
        this.afterRead();
        return schema;
    }

    /**
     * Currently doesn't allow to exist schema with the same name
     * @param type the query schema type
     * @param name the query schema name
     * @param <T>  SubClass of SchemaElement
     * @return     the queried schema object
     */
    protected <T extends SchemaElement> T getSchema(BigType type,
                                                    String name) {
        LOG.debug("SchemaTransaction get {} by name '{}'",
                  type.readableName(), name);
        this.beforeRead();

        ConditionQuery query = new ConditionQuery(type);
        query.eq(BigKeys.NAME, name);
        QueryResults<BackendEntry> results = this.indexTx.query(query);

        this.afterRead();

        // Should not exist schema with same name
        BackendEntry entry = results.one();
        if (entry == null) {
            return null;
        }
        T schema = this.deserialize(entry, type);
        return schema;
    }

    protected <T extends SchemaElement> List<T> getAllSchema(BigType type) {
        List<T> results = new ArrayList<>();
        Query query = new Query(type);
        Iterator<BackendEntry> entries = this.query(query).iterator();
        try {
            while (entries.hasNext()) {
                results.add(this.deserialize(entries.next(), type));
                Query.checkForceCapacity(results.size());
            }
        } finally {
            CloseableIterator.closeIterator(entries);
        }
        return results;
    }

    protected void removeSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction remove {} by id '{}'",
                  schema.type(), schema.id());
        LockUtil.Locks locks = new LockUtil.Locks(this.graphName());
        try {
            locks.lockWrites(LockUtil.hugeType2Group(schema.type()),
                             schema.id());
            this.beforeWrite();
            this.indexTx.updateNameIndex(schema, true);
            BackendEntry e = this.serializer.writeId(schema.type(), schema.id());
            this.doRemove(e);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
    }

    private BackendEntry serialize(SchemaElement schema) {
        switch (schema.type()) {
            case PROPERTY_KEY:
                return this.serializer.writePropertyKey((PropertyKey) schema);
            case VERTEX_LABEL:
                return this.serializer.writeVertexLabel((VertexLabel) schema);
            case EDGE_LABEL:
                return this.serializer.writeEdgeLabel((EdgeLabel) schema);
            case INDEX_LABEL:
                return this.serializer.writeIndexLabel((IndexLabel) schema);
            default:
                throw new AssertionError(String.format(
                          "Unknown schema type '%s'", schema.type()));
        }
    }

    @SuppressWarnings({"unchecked"})
    private <T> T deserialize(BackendEntry entry, BigType type) {
        switch (type) {
            case PROPERTY_KEY:
                return (T) this.serializer.readPropertyKey(this.graph(), entry);
            case VERTEX_LABEL:
                return (T) this.serializer.readVertexLabel(this.graph(), entry);
            case EDGE_LABEL:
                return (T) this.serializer.readEdgeLabel(this.graph(), entry);
            case INDEX_LABEL:
                return (T) this.serializer.readIndexLabel(this.graph(), entry);
            default:
                throw new AssertionError(String.format(
                          "Unknown schema type '%s'", type));
        }
    }

    public void checkSchemaName(String name) {
        String illegalReg = this.params().configuration()
                                .get(CoreOptions.SCHEMA_ILLEGAL_NAME_REGEX);

        E.checkNotNull(name, "name");
        E.checkArgument(!name.isEmpty(), "The name can't be empty.");
        E.checkArgument(name.length() < 256,
                        "The length of name must less than 256 bytes.");
        E.checkArgument(!name.matches(illegalReg),
                        "Illegal schema name '%s'", name);

        final char[] filters = {'#', '>', ':', '!'};
        for (char c : filters) {
            E.checkArgument(name.indexOf(c) == -1,
                            "The name can't contain character '%s'.", c);
        }
    }

    @Watched(prefix = "schema")
    public Id validOrGenerateId(BigType type, Id id, String name) {
        boolean forSystem = Graph.Hidden.isHidden(name);
        if (id != null) {
            this.checkIdAndUpdateNextId(type, id, name, forSystem);
        } else {
            if (forSystem) {
                id = this.getNextSystemId();
            } else {
                id = this.getNextId(type);
            }
        }
        return id;
    }

    private void checkIdAndUpdateNextId(BigType type, Id id,
                                        String name, boolean forSystem) {
        if (forSystem) {
            if (id.number() && id.asLong() < 0) {
                return;
            }
            throw new IllegalStateException(String.format(
                      "Invalid system id '%s'", id));
        }
        E.checkState(id.number() && id.asLong() > 0L,
                     "Schema id must be number and >0, but got '%s'", id);
        GraphMode mode = this.graphMode();
        E.checkState(mode == GraphMode.RESTORING,
                     "Can't build schema with provided id '%s' " +
                     "when graph '%s' in mode '%s'",
                     id, this.graphName(), mode);
        this.setNextIdLowest(type, id.asLong());
    }

    @Watched(prefix = "schema")
    public Id getNextId(BigType type) {
        LOG.debug("SchemaTransaction get next id for {}", type);
        return this.store().nextId(type);
    }

    @Watched(prefix = "schema")
    public void setNextIdLowest(BigType type, long lowest) {
        LOG.debug("SchemaTransaction set next id to {} for {}", lowest, type);
        this.store().setCounterLowest(type, lowest);
    }

    @Watched(prefix = "schema")
    public Id getNextSystemId() {
        LOG.debug("SchemaTransaction get next system id");
        Id id = this.store().nextId(BigType.SYS_SCHEMA);
        return IdGenerator.of(-id.asLong());
    }

    private static void setCreateTimeIfNeeded(SchemaElement schema) {
        if (!schema.userdata().containsKey(Userdata.CREATE_TIME)) {
            schema.userdata(Userdata.CREATE_TIME, DateUtil.now());
        }
    }

    private static Id asyncRun(BigGraph graph, SchemaElement schema,
                               SchemaCallable callable) {
        return asyncRun(graph, schema, callable, ImmutableSet.of());
    }

    @Watched(prefix = "schema")
    private static Id asyncRun(BigGraph graph, SchemaElement schema,
                               SchemaCallable callable, Set<Id> dependencies) {
        E.checkArgument(schema != null, "Schema can't be null");
        String name = SchemaCallable.formatTaskName(schema.type(),
                                                    schema.id(),
                                                    schema.name());

        JobBuilder<Object> builder = JobBuilder.of(graph).name(name)
                                               .job(callable)
                                               .dependencies(dependencies);
        BigTask<?> task = builder.schedule();

        // If TASK_SYNC_DELETION is true, wait async thread done before
        // continue. This is used when running tests.
        if (graph.option(CoreOptions.TASK_SYNC_DELETION)) {
            task.syncWait();
        }
        return task.id();
    }
}
