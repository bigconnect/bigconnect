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
package com.mware.ge.store;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.mware.core.cache.CacheOptions;
import com.mware.core.cache.CacheService;
import com.mware.core.cache.InMemoryCacheService;
import com.mware.core.util.StreamUtil;
import com.mware.ge.*;
import com.mware.ge.event.*;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.id.SimpleNameSubstitutionStrategy;
import com.mware.ge.mutation.*;
import com.mware.ge.property.MutableProperty;
import com.mware.ge.property.PropertyDescriptor;
import com.mware.ge.search.IndexHint;
import com.mware.ge.security.ColumnVisibility;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.store.mutations.ElementMutationBuilder;
import com.mware.ge.store.mutations.StoreMutation;
import com.mware.ge.store.util.StorableKeyHelper;
import com.mware.ge.store.util.StreamingPropertyValueStorageStrategy;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IncreasingTime;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValueRef;

import java.io.InputStream;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mware.ge.util.IterableUtils.singleOrDefault;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.util.Preconditions.checkNotNull;
import static com.mware.ge.util.StreamUtils.stream;

public abstract class AbstractStorableGraph<V extends StorableVertex, E extends StorableEdge> extends GraphBaseWithSearchIndex implements StorableGraph<V, E> {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(AbstractStorableGraph.class);
    private static final String METADATA_GRAPH_VERSION_KEY = "graph.version";
    private static final Integer METADATA_GRAPH_VERSION = 3;
    private static final String METADATA_SERIALIZER = "graph.serializer";
    private static final String METADATA_STREAMING_PROPERTY_VALUE_DATA_WRITER = "graph.streamingPropertyValueStorageStrategy";
    private final String VERTEX_CACHE_NAME = "v";
    private final String EDGE_CACHE_NAME = "e";

    protected static String verticesTableName;
    protected static String historyVerticesTableName;
    protected static String edgesTableName;
    protected static String historyEdgesTableName;
    protected static String extendedDataTableName;
    protected static String dataTableName;
    protected static String metadataTableName;
    protected final Queue<GraphEvent> graphEventQueue = new LinkedList<>();
    protected final ElementMutationBuilder elementMutationBuilder;
    protected NameSubstitutionStrategy nameSubstitutionStrategy;

    private Integer graphVersion;
    protected final StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy;
    protected final GeSerializer geSerializer;
    protected final boolean cacheEnabled;
    private boolean foundGeSerializerMetadata;
    private boolean foundStreamingPropertyValueStorageStrategyMetadata;
    private final boolean historyInSeparateTable;
    protected GraphMetadataStore graphMetadataStore;

    protected final CacheService elementCacheService;
    protected final CacheOptions elementCacheOptions;

    protected AbstractStorableGraph(StorableGraphConfiguration config) {
        super(config);

        this.historyInSeparateTable = config.isHistoryInSeparateTable();
        this.nameSubstitutionStrategy = new SimpleNameSubstitutionStrategy();

        verticesTableName = getVerticesTableName(getConfiguration().getTableNamePrefix());
        edgesTableName = getEdgesTableName(getConfiguration().getTableNamePrefix());
        extendedDataTableName = getExtendedDataTableName(getConfiguration().getTableNamePrefix());
        dataTableName = getDataTableName(getConfiguration().getTableNamePrefix());
        metadataTableName = getMetadataTableName(getConfiguration().getTableNamePrefix());
        if (isHistoryInSeparateTable()) {
            historyVerticesTableName = getHistoryVerticesTableName(getConfiguration().getTableNamePrefix());
            historyEdgesTableName = getHistoryEdgesTableName(getConfiguration().getTableNamePrefix());
        } else {
            historyVerticesTableName = null;
            historyEdgesTableName = null;
        }

        this.geSerializer = config.createSerializer(this);
        this.streamingPropertyValueStorageStrategy = config.createStreamingPropertyValueStorageStrategy(this);
        this.cacheEnabled = config.isElementCacheEnabled();

        this.elementCacheService = new InMemoryCacheService();
        this.elementCacheOptions = new CacheOptions()
                .setMaximumSize((long) config.getElementCacheSize());

        this.elementMutationBuilder = new ElementMutationBuilder(streamingPropertyValueStorageStrategy, this, geSerializer) {
            @Override
            protected void saveVertexMutation(StoreMutation m) {
                addMutations(GeObjectType.VERTEX, m);
            }

            @Override
            protected void saveEdgeMutation(StoreMutation m) {
                addMutations(GeObjectType.EDGE, m);
            }

            @Override
            protected void saveExtendedDataMutation(ElementType elementType, StoreMutation m) {
                addMutations(GeObjectType.EXTENDED_DATA, m);
            }

            @Override
            protected NameSubstitutionStrategy getNameSubstitutionStrategy() {
                return AbstractStorableGraph.this.getNameSubstitutionStrategy();
            }

            @Override
            public void saveDataMutation(StoreMutation m) {
                addMutations(GeObjectType.STREAMING_DATA, m);
            }

            @Override
            @SuppressWarnings("unchecked")
            protected StreamingPropertyValueRef saveStreamingPropertyValue(String rowKey, Property property, StreamingPropertyValue propertyValue) {
                StreamingPropertyValueRef streamingPropertyValueRef = super.saveStreamingPropertyValue(rowKey, property, propertyValue);
                ((MutableProperty) property).setValue(streamingPropertyValueRef.toStreamingPropertyValue(AbstractStorableGraph.this, property.getTimestamp()));
                return streamingPropertyValueRef;
            }
        };
    }

    @Override
    protected void setup() {
        super.setup();
        if (graphVersion == null) {
            setMetadata(METADATA_GRAPH_VERSION_KEY, METADATA_GRAPH_VERSION);
        } else if (!METADATA_GRAPH_VERSION.equals(graphVersion)) {
            throw new GeException("Invalid accumulo graph version. Expected " + METADATA_GRAPH_VERSION + " found " + graphVersion);
        }
    }

    @Override
    protected void setupGraphMetadata() {
        foundGeSerializerMetadata = false;
        super.setupGraphMetadata();
        if (!foundGeSerializerMetadata) {
            setMetadata(METADATA_SERIALIZER, geSerializer.getClass().getName());
        }
        if (!foundStreamingPropertyValueStorageStrategyMetadata) {
            setMetadata(METADATA_STREAMING_PROPERTY_VALUE_DATA_WRITER, streamingPropertyValueStorageStrategy.getClass().getName());
        }
    }

    @Override
    protected void setupGraphMetadata(GraphMetadataEntry graphMetadataEntry) {
        super.setupGraphMetadata(graphMetadataEntry);
        if (graphMetadataEntry.getKey().equals(METADATA_GRAPH_VERSION_KEY)) {
            if (graphMetadataEntry.getValue() instanceof Integer) {
                graphVersion = (Integer) graphMetadataEntry.getValue();
                LOGGER.debug("Metadata: %s=%s", METADATA_GRAPH_VERSION_KEY, graphVersion);
            } else {
                throw new GeException("Invalid accumulo version in metadata. " + graphMetadataEntry);
            }
        } else if (graphMetadataEntry.getKey().equals(METADATA_SERIALIZER)) {
            validateClassMetadataEntry(graphMetadataEntry, geSerializer.getClass());
            foundGeSerializerMetadata = true;
        } else if (graphMetadataEntry.getKey().equals(METADATA_STREAMING_PROPERTY_VALUE_DATA_WRITER)) {
            validateClassMetadataEntry(graphMetadataEntry, streamingPropertyValueStorageStrategy.getClass());
            foundStreamingPropertyValueStorageStrategyMetadata = true;
        }
    }

    private void validateClassMetadataEntry(GraphMetadataEntry graphMetadataEntry, Class expectedClass) {
        if (!(graphMetadataEntry.getValue() instanceof String)) {
            LOGGER.error("Invalid " + graphMetadataEntry.getKey() + " expected string found " + graphMetadataEntry.getValue().getClass().getName());
        }
        String foundClassName = (String) graphMetadataEntry.getValue();
        if (!foundClassName.equals(expectedClass.getName())) {
            LOGGER.error("Invalid " + graphMetadataEntry.getKey() + " expected " + foundClassName + " found " + expectedClass.getName());
        }
    }

    @Override
    public StorableVertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility, String conceptType) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalVertexId = vertexId;

        return new StorableVertexBuilder(finalVertexId, conceptType, visibility, elementMutationBuilder) {
            @Override
            public Vertex save(Authorizations authorizations) {
                // This has to occur before createVertex since it will mutate the properties
                getElementMutationBuilder().saveVertexBuilder(AbstractStorableGraph.this, this, timestampLong);

                StorableVertex vertex = createVertex(authorizations);

                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    getSearchIndex().addElement(
                            AbstractStorableGraph.this,
                            vertex,
                            authorizations
                    );
                    getSearchIndex().addElementExtendedData(
                            AbstractStorableGraph.this,
                            vertex,
                            getExtendedData(),
                            authorizations
                    );

                    for (ExtendedDataDeleteMutation m : getExtendedDataDeletes()) {
                        getSearchIndex().deleteExtendedData(
                                AbstractStorableGraph.this,
                                vertex,
                                m.getTableName(),
                                m.getRow(),
                                m.getColumnName(),
                                m.getKey(),
                                m.getVisibility(),
                                authorizations
                        );
                    }
                }

                if (hasEventListeners()) {
                    queueEvent(new AddVertexEvent(AbstractStorableGraph.this, vertex));
                    queueEvents(
                            vertex,
                            getProperties(),
                            getPropertyDeletes(),
                            getPropertySoftDeletes(),
                            getExtendedData(),
                            getExtendedDataDeletes()
                    );
                }

                if (cacheEnabled) {
                    elementCacheService.put(VERTEX_CACHE_NAME, finalVertexId, vertex, elementCacheOptions);
                }

                return vertex;
            }

            @Override
            protected StorableVertex createVertex(Authorizations authorizations) {
                Iterable<Visibility> hiddenVisibilities = null;
                String conceptType = getConceptType();
                if (getNewConceptType() != null)
                    conceptType = getNewConceptType();

                return new StorableVertex(
                        AbstractStorableGraph.this,
                        getId(),
                        conceptType,
                        null,
                        getVisibility(),
                        getProperties(),
                        getPropertyDeletes(),
                        getPropertySoftDeletes(),
                        hiddenVisibilities,
                        getExtendedDataTableNames(),
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        authorizations
                );
            }
        };
    }

    @Override
    public void saveProperties(
            StorableElement element,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeletes,
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes
    ) {
        String elementRowKey = element.getId();
        StoreMutation m = new StoreMutation(elementRowKey);
        boolean hasProperty = false;
        for (PropertyDeleteMutation propertyDelete : propertyDeletes) {
            hasProperty = true;
            elementMutationBuilder.addPropertyDeleteToMutation(m, propertyDelete);
        }
        for (PropertySoftDeleteMutation propertySoftDelete : propertySoftDeletes) {
            hasProperty = true;
            elementMutationBuilder.addPropertySoftDeleteToMutation(m, propertySoftDelete);
        }
        for (Property property : properties) {
            hasProperty = true;
            elementMutationBuilder.addPropertyToMutation(this, m, elementRowKey, property);
            if (property.getValue() instanceof StreamingPropertyValue) {
                // We need to specifically update the property on the element after transformValue is called because
                // on add or set property we are creating a DefaultStreamingPropertyValue which has been consumed at
                // this point. The transformValue method in `addPropertyToMutation` creates a new InputSteam based
                // on the strategy.
                element.addPropertyInternal(property);
            }
        }
        if (hasProperty) {
            addMutations(element, m);
        }

        if (hasEventListeners()) {
            queueEvents(
                    element,
                    properties,
                    propertyDeletes,
                    propertySoftDeletes,
                    null,
                    null
            );
        }
    }

    @Override
    public void deleteProperty(StorableElement element, Property property, Authorizations authorizations) {
        if (!element.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
            throw new GeMissingFetchHintException(element.getFetchHints(), "Property " + property.getName() + " needs to be included with metadata");
        }

        StoreMutation m = new StoreMutation(element.getId());
        elementMutationBuilder.addPropertyDeleteToMutation(m, property);
        addMutations(element, m);

        getSearchIndex().deleteProperty(
                this,
                element,
                PropertyDescriptor.fromProperty(property),
                authorizations
        );

        if (hasEventListeners()) {
            queueEvent(new DeletePropertyEvent(this, element, property));
        }
    }


    @Override
    public void softDeleteProperty(StorableElement element, Property property, Authorizations authorizations) {
        StoreMutation m = new StoreMutation(element.getId());
        elementMutationBuilder.addPropertySoftDeleteToMutation(m, property);
        addMutations(element, m);

        getSearchIndex().deleteProperty(
                this,
                element,
                PropertyDescriptor.fromProperty(property),
                authorizations
        );

        if (hasEventListeners()) {
            queueEvent(new SoftDeletePropertyEvent(this, element, property));
        }
    }

    @Override
    public void softDeleteProperties(Iterable<Property> properties, StorableElement element, Authorizations authorizations) {
        StreamUtil.stream(properties)
                .forEach(p -> softDeleteProperty(element, p, authorizations));
    }

    protected void addMutations(Element element, StoreMutation... mutations) {
        addMutations(GeObjectType.getTypeFromElement(element), mutations);
    }

    protected abstract void addMutations(GeObjectType objectType, StoreMutation... mutations);

    private void queueEvents(
            Element element,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeletes,
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes,
            Iterable<ExtendedDataMutation> extendedData,
            Iterable<ExtendedDataDeleteMutation> extendedDataDeletes
    ) {
        if (properties != null) {
            for (Property property : properties) {
                queueEvent(new AddPropertyEvent(this, element, property));
            }
        }
        if (propertyDeletes != null) {
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeletes) {
                queueEvent(new DeletePropertyEvent(this, element, propertyDeleteMutation));
            }
        }
        if (propertySoftDeletes != null) {
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeletes) {
                queueEvent(new SoftDeletePropertyEvent(this, element, propertySoftDeleteMutation));
            }
        }
        if (extendedData != null) {
            for (ExtendedDataMutation extendedDataMutation : extendedData) {
                queueEvent(new AddExtendedDataEvent(
                        this,
                        element,
                        extendedDataMutation.getTableName(),
                        extendedDataMutation.getRow(),
                        extendedDataMutation.getColumnName(),
                        extendedDataMutation.getKey(),
                        extendedDataMutation.getValue(),
                        extendedDataMutation.getVisibility()
                ));
            }
        }
        if (extendedDataDeletes != null) {
            for (ExtendedDataDeleteMutation extendedDataDeleteMutation : extendedDataDeletes) {
                queueEvent(new DeleteExtendedDataEvent(
                        this,
                        element,
                        extendedDataDeleteMutation.getTableName(),
                        extendedDataDeleteMutation.getRow(),
                        extendedDataDeleteMutation.getColumnName(),
                        extendedDataDeleteMutation.getKey()
                ));
            }
        }
    }

    @Override
    public Iterable<Vertex> getVertices(FetchHints fetchHints, Long endTime, Authorizations authorizations) throws GeException {
        return getVerticesInRange(new IdRange(null, null), fetchHints, endTime, authorizations);
    }

    @Override
    public void deleteElements(Stream<? extends ElementId> elementIds, Authorizations authorizations) {
        DeleteElementsConsumer consumer = new DeleteElementsConsumer(authorizations);
        elementIds.forEach(consumer);
        consumer.processBatches(true);
    }

    @Override
    public void deleteVertex(Vertex vertex, Authorizations authorizations) {
        if (cacheEnabled) {
            elementCacheService.invalidate(VERTEX_CACHE_NAME, vertex.getId());
        }
        deleteElements(Stream.of(vertex), authorizations);
    }

    @Override
    public void deleteVertex(String vertexId, Authorizations authorizations) {
        if (cacheEnabled) {
            elementCacheService.invalidate(VERTEX_CACHE_NAME, vertexId);
        }
        deleteElements(Stream.of(vertexId).map(ElementId::vertex), authorizations);
    }

    @Override
    public void deleteEdge(String edgeId, Authorizations authorizations) {
        if (cacheEnabled) {
            elementCacheService.invalidate(EDGE_CACHE_NAME, edgeId);
        }
        deleteElements(Stream.of(edgeId).map(ElementId::edge), authorizations);
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");

        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        getSearchIndex().deleteElement(this, vertex, authorizations);

        // Delete all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
            softDeleteEdge(edge, timestamp, authorizations);
        }

        addMutations(GeObjectType.VERTEX, getSoftDeleteRowMutation(vertex.getId(), timestamp));

        if (hasEventListeners()) {
            queueEvent(new SoftDeleteVertexEvent(this, vertex));
        }
    }

    private StoreMutation getSoftDeleteRowMutation(String rowKey, long timestamp) {
        StoreMutation m = new StoreMutation(rowKey);
        m.put(StorableElement.CF_SOFT_DELETE, StorableElement.CQ_SOFT_DELETE, timestamp, StorableElement.SOFT_DELETE_VALUE);
        return m;
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        ColumnVisibility columnVisibility = ElementMutationBuilder.visibilityToColumnVisibility(visibility);

        // Delete all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
            markEdgeHidden(edge, visibility, authorizations);
        }

        addMutations(GeObjectType.VERTEX, getMarkHiddenRowMutation(vertex.getId(), columnVisibility));

        getSearchIndex().markElementHidden(this, vertex, visibility, authorizations);

        if (hasEventListeners()) {
            queueEvent(new MarkHiddenVertexEvent(this, vertex));
        }
    }

    private StoreMutation getMarkHiddenRowMutation(String rowKey, ColumnVisibility visibility) {
        StoreMutation m = new StoreMutation(rowKey);
        m.put(StorableElement.CF_HIDDEN, StorableElement.CQ_HIDDEN, visibility, StorableElement.HIDDEN_VALUE);
        return m;
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        ColumnVisibility columnVisibility = ElementMutationBuilder.visibilityToColumnVisibility(visibility);

        // Delete all edges that this vertex participates.
        for (Edge edge : vertex.getEdges(Direction.BOTH, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
            markEdgeVisible(edge, visibility, authorizations);
        }

        addMutations(GeObjectType.VERTEX, getMarkVisibleRowMutation(vertex.getId(), columnVisibility));

        getSearchIndex().markElementVisible(this, vertex, visibility, authorizations);

        if (hasEventListeners()) {
            queueEvent(new MarkVisibleVertexEvent(this, vertex));
        }
    }

    private StoreMutation getMarkVisibleRowMutation(String rowKey, ColumnVisibility visibility) {
        StoreMutation m = new StoreMutation(rowKey);
        m.putDelete(StorableElement.CF_HIDDEN, StorableElement.CQ_HIDDEN, visibility);
        return m;
    }

    @Override
    public List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        if (streamingPropertyValues.size() == 0) {
            return Collections.emptyList();
        }
        return streamingPropertyValueStorageStrategy.getInputStreams(streamingPropertyValues);
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, FetchHints fetchHints, Authorizations authorizations) {
        List<IdRange> ranges = extendedDataRowIdToRange(ids);
        return getExtendedDataRowsInRange(ranges, fetchHints, authorizations);
    }

    protected abstract Iterable<ExtendedDataRow> getExtendedDataRowsInRange(
            List<IdRange> ranges,
            FetchHints fetchHints,
            Authorizations authorizations
    );

    @Override
    public Iterable<ExtendedDataRow> getExtendedDataForElements(
            Iterable<? extends ElementId> elementIdsArg,
            String tableName,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        List<? extends ElementId> elementIds = toList(elementIdsArg);
        try {
            List<IdRange> ranges = elementIds.stream()
                    .map(elementId -> IdRange.prefix(
                            StorableKeyHelper.createExtendedDataRowKey(elementId.getElementType(),
                                    elementId.getId(),
                                    tableName,
                                    null
                            )))
                    .collect(Collectors.toList());
            if (ranges.size() == 0) {
                return Collections.emptyList();
            }
            return getExtendedDataRowsInRange(ranges, fetchHints, authorizations);
        } catch (IllegalStateException ex) {
            throw new GeException("Failed to get extended data: " + Joiner.on(", ").join(elementIds) + ":" + tableName, ex);
        }
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, IdRange elementIdRange, Authorizations authorizations) {
        IdRange extendedDataRowKeyRange = StorableKeyHelper.createExtendedDataRowKeyRange(elementType, elementIdRange);
        return getExtendedDataInRange(extendedDataRowKeyRange, authorizations);
    }

    public Iterable<ExtendedDataRow> getExtendedDataInRange(IdRange extendedDataRowKeyRange, Authorizations authorizations) {
        return getExtendedDataRowsInRange(Collections.singletonList(extendedDataRowKeyRange), FetchHints.ALL, authorizations);
    }

    private List<IdRange> extendedDataRowIdToRange(Iterable<ExtendedDataRowId> ids) {
        return stream(ids)
                .map(id -> IdRange.prefix(StorableKeyHelper.createExtendedDataRowKey(id)))
                .collect(Collectors.toList());
    }

    public void saveExtendedDataMutations(
            Element element,
            ElementType elementType,
            IndexHint indexHint,
            Iterable<ExtendedDataMutation> extendedData,
            Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
            Authorizations authorizations
    ) {
        if (extendedData == null) {
            return;
        }

        String elementId = element.getId();
        elementMutationBuilder.saveExtendedDataMarkers(elementId, elementType, extendedData);
        elementMutationBuilder.saveExtendedData(
                this,
                elementId,
                elementType,
                extendedData,
                extendedDataDeletes
        );

        if (indexHint != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().addElementExtendedData(this, element, extendedData, authorizations);
            for (ExtendedDataDeleteMutation m : extendedDataDeletes) {
                getSearchIndex().deleteExtendedData(
                        this,
                        element,
                        m.getTableName(),
                        m.getRow(),
                        m.getColumnName(),
                        m.getKey(),
                        m.getVisibility(),
                        authorizations
                );
            }
        }

        if (hasEventListeners()) {
            queueEvents(
                    element,
                    null,
                    null,
                    null,
                    extendedData,
                    extendedDataDeletes
            );
        }
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Edge edge;
        if (cacheEnabled) {
            edge = elementCacheService.getIfPresent(EDGE_CACHE_NAME, edgeId);
            if (edge != null && edge.getFetchHints().hasFetchHints(fetchHints) && authorizations.contains(edge.getAuthorizations()))
                return edge;
        }

        try {
            edge = singleOrDefault(getEdgesInRange(new IdRange(edgeId, true, edgeId, true), fetchHints, endTime, authorizations), null);
            if (edge != null && cacheEnabled) {
                elementCacheService.put(EDGE_CACHE_NAME, edgeId, edge, elementCacheOptions);
            }
            return edge;
        } catch (IllegalStateException ex) {
            throw new GeException("Failed to find edge with id: " + edgeId, ex);
        }
    }

    @Override
    public Iterable<Edge> getEdges(FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return getEdgesInRange(null, fetchHints, endTime, authorizations);
    }

    public abstract Iterable<Edge> getEdgesInRange(
            final IdRange range,
            final FetchHints fetchHints,
            final Long endTime,
            final Authorizations authorizations
    );

    @Override
    public StorableEdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility) {
        checkNotNull(outVertexId, "outVertexId cannot be null");
        checkNotNull(inVertexId, "inVertexId cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalEdgeId = edgeId;
        return new StorableEdgeBuilderByVertexId(finalEdgeId, outVertexId, inVertexId, label, visibility, elementMutationBuilder) {
            @Override
            public Edge save(Authorizations authorizations) {
                // This has to occur before createEdge since it will mutate the properties
                elementMutationBuilder.saveEdgeBuilder(AbstractStorableGraph.this, this, timestampLong);

                StorableEdge edge = AbstractStorableGraph.this.createEdge(
                        this,
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        authorizations
                );
                return savePreparedEdge(this, edge, null, authorizations);
            }

            @Override
            protected StorableEdge createEdge(Authorizations authorizations) {
                return AbstractStorableGraph.this.createEdge(
                        this,
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        authorizations
                );
            }
        };
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        checkNotNull(outVertex, "outVertex cannot be null");
        checkNotNull(inVertex, "inVertex cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalEdgeId = edgeId;
        return new EdgeBuilder(finalEdgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                AddEdgeToVertexRunnable addEdgeToVertex = new AddEdgeToVertexRunnable() {
                    @Override
                    public void run(StorableEdge edge) {
                        if (getOutVertex() instanceof StorableVertex) {
                            ((StorableVertex) getOutVertex()).addOutEdge(edge);
                        }
                        if (getInVertex() instanceof StorableVertex) {
                            ((StorableVertex) getInVertex()).addInEdge(edge);
                        }
                    }
                };

                // This has to occur before createEdge since it will mutate the properties
                elementMutationBuilder.saveEdgeBuilder(AbstractStorableGraph.this, this, timestampLong);

                StorableEdge edge = createEdge(
                        this,
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        authorizations
                );
                return savePreparedEdge(this, edge, addEdgeToVertex, authorizations);
            }
        };
    }

    protected StorableEdge createEdge(
            EdgeBuilderBase edgeBuilder,
            long timestamp,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        Iterable<Visibility> hiddenVisibilities = null;
        String edgeLabel = edgeBuilder.getEdgeLabel();
        if (edgeBuilder.getNewEdgeLabel() != null)
            edgeLabel = edgeBuilder.getNewEdgeLabel();

        return new StorableEdge(
                this,
                edgeBuilder.getId(),
                edgeBuilder.getVertexId(Direction.OUT),
                edgeBuilder.getVertexId(Direction.IN),
                edgeLabel,
                null,
                edgeBuilder.getVisibility(),
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes(),
                hiddenVisibilities,
                edgeBuilder.getExtendedDataTableNames(),
                timestamp,
                fetchHints,
                authorizations
        );
    }

    private Edge savePreparedEdge(
            EdgeBuilderBase edgeBuilder,
            StorableEdge edge,
            AddEdgeToVertexRunnable addEdgeToVertex,
            Authorizations authorizations
    ) {
        if (addEdgeToVertex != null) {
            addEdgeToVertex.run(edge);
        }

        if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().addElement(AbstractStorableGraph.this, edge, authorizations);
            getSearchIndex().addElementExtendedData(AbstractStorableGraph.this, edge, edgeBuilder.getExtendedData(), authorizations);
            for (ExtendedDataDeleteMutation m : edgeBuilder.getExtendedDataDeletes()) {
                getSearchIndex().deleteExtendedData(
                        AbstractStorableGraph.this,
                        edge,
                        m.getTableName(),
                        m.getRow(),
                        m.getColumnName(),
                        m.getKey(),
                        m.getVisibility(),
                        authorizations
                );
            }
        }

        if (hasEventListeners()) {
            queueEvent(new AddEdgeEvent(this, edge));
            queueEvents(
                    edge,
                    edgeBuilder.getProperties(),
                    edgeBuilder.getPropertyDeletes(),
                    edgeBuilder.getPropertySoftDeletes(),
                    edgeBuilder.getExtendedData(),
                    edgeBuilder.getExtendedDataDeletes()
            );
        }

        if (cacheEnabled) {
            elementCacheService.invalidate(EDGE_CACHE_NAME, edge.getId());
            elementCacheService.invalidate(VERTEX_CACHE_NAME, edgeBuilder.getVertexId(Direction.OUT));
            elementCacheService.invalidate(VERTEX_CACHE_NAME, edgeBuilder.getVertexId(Direction.IN));
        }

        return edge;
    }

    @Override
    public void deleteExtendedDataRow(ExtendedDataRowId rowId, Authorizations authorizations) {
        checkNotNull(rowId);
        getSearchIndex().deleteExtendedData(this, rowId, authorizations);

        addMutations(GeObjectType.EXTENDED_DATA, getDeleteExtendedDataMutations(rowId));

        if (hasEventListeners()) {
            queueEvent(new DeleteExtendedDataRowEvent(this, rowId));
        }
    }

    @Override
    public void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations) {
        checkNotNull(edge);
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        getSearchIndex().deleteElement(this, edge, authorizations);

        ColumnVisibility visibility = ElementMutationBuilder.visibilityToColumnVisibility(edge.getVisibility());

        StoreMutation outMutation = new StoreMutation(edge.getVertexId(Direction.OUT));
        outMutation.put(StorableVertex.CF_OUT_EDGE_SOFT_DELETE, edge.getId(), visibility, timestamp, StorableElement.SOFT_DELETE_VALUE);

        StoreMutation inMutation = new StoreMutation(edge.getVertexId(Direction.IN));
        inMutation.put(StorableVertex.CF_IN_EDGE_SOFT_DELETE, edge.getId(), visibility, timestamp, StorableElement.SOFT_DELETE_VALUE);

        addMutations(GeObjectType.VERTEX, outMutation, inMutation);

        // Soft deletes everything else related to edge.
        addMutations(GeObjectType.EDGE, getSoftDeleteRowMutation(edge.getId(), timestamp));

        if (hasEventListeners()) {
            queueEvent(new SoftDeleteEdgeEvent(this, edge));
        }
    }

    @Override
    public void deleteEdge(Edge edge, Authorizations authorizations) {
        deleteElements(Stream.of(edge), authorizations);
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        checkNotNull(edge);
        Vertex out = edge.getVertex(Direction.OUT, authorizations);
        if (out == null) {
            throw new GeException(String.format("Unable to mark edge hidden %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
        }
        Vertex in = edge.getVertex(Direction.IN, authorizations);
        if (in == null) {
            throw new GeException(String.format("Unable to mark edge hidden %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
        }

        ColumnVisibility columnVisibility = ElementMutationBuilder.visibilityToColumnVisibility(visibility);

        StoreMutation outMutation = new StoreMutation(out.getId());
        outMutation.put(StorableVertex.CF_OUT_EDGE_HIDDEN, edge.getId(), columnVisibility, StorableElement.HIDDEN_VALUE);

        StoreMutation inMutation = new StoreMutation(in.getId());
        inMutation.put(StorableVertex.CF_IN_EDGE_HIDDEN, edge.getId(), columnVisibility, StorableElement.HIDDEN_VALUE);

        addMutations(GeObjectType.VERTEX, outMutation, inMutation);

        // Delete everything else related to edge.
        addMutations(GeObjectType.EDGE, getMarkHiddenRowMutation(edge.getId(), columnVisibility));

        if (out instanceof StorableVertex) {
            ((StorableVertex) out).removeOutEdge(edge);
        }
        if (in instanceof StorableVertex) {
            ((StorableVertex) in).removeInEdge(edge);
        }

        getSearchIndex().markElementHidden(this, edge, visibility, authorizations);

        if (hasEventListeners()) {
            queueEvent(new MarkHiddenEdgeEvent(this, edge));
        }
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        checkNotNull(edge);
        Vertex out = edge.getVertex(Direction.OUT, FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        if (out == null) {
            throw new GeException(String.format("Unable to mark edge visible %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
        }
        Vertex in = edge.getVertex(Direction.IN, FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        if (in == null) {
            throw new GeException(String.format("Unable to mark edge visible %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
        }

        ColumnVisibility columnVisibility = ElementMutationBuilder.visibilityToColumnVisibility(visibility);

        StoreMutation outMutation = new StoreMutation(out.getId());
        outMutation.putDelete(StorableVertex.CF_OUT_EDGE_HIDDEN, edge.getId(), columnVisibility);

        StoreMutation inMutation = new StoreMutation(in.getId());
        inMutation.putDelete(StorableVertex.CF_IN_EDGE_HIDDEN, edge.getId(), columnVisibility);

        addMutations(GeObjectType.VERTEX, outMutation, inMutation);

        // Delete everything else related to edge.
        addMutations(GeObjectType.EDGE, getMarkVisibleRowMutation(edge.getId(), columnVisibility));

        if (out instanceof StorableVertex) {
            ((StorableVertex) out).addOutEdge(edge);
        }
        if (in instanceof StorableVertex) {
            ((StorableVertex) in).addInEdge(edge);
        }

        getSearchIndex().markElementVisible(this, edge, visibility, authorizations);

        if (hasEventListeners()) {
            queueEvent(new MarkVisibleEdgeEvent(this, edge));
        }
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        return new Authorizations(auths);
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, Authorizations authorizations) throws GeException {
        try {
            if (vertexId == null) {
                return null;
            }

            Vertex vertex;
            if (cacheEnabled) {
                vertex = elementCacheService.getIfPresent(VERTEX_CACHE_NAME, vertexId);
                if (vertex != null && vertex.getFetchHints().hasFetchHints(fetchHints) && authorizations.contains(vertex.getAuthorizations()))
                    return vertex;
            }

            vertex = singleOrDefault(getVerticesInRange(new IdRange(vertexId), fetchHints, endTime, authorizations), null);
            if (vertex != null && cacheEnabled) {
                elementCacheService.put(VERTEX_CACHE_NAME, vertexId, vertex, elementCacheOptions);
            }
            return vertex;
        } catch (IllegalStateException ex) {
            throw new GeException("Failed to find vertex with id: " + vertexId, ex);
        }
    }

    @Override
    public Iterable<String> getVertexIds(Authorizations authorizations) {
        return new ConvertingIterable<Vertex, String>(getVertices(FetchHints.NONE, authorizations)) {

            @Override
            protected String convert(Vertex o) {
                return o.getId();
            }
        };
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        IdRange range = IdRange.prefix(vertexIdPrefix);
        return getVerticesInRange(range, fetchHints, endTime, authorizations);
    }

    @Override
    public abstract Iterable<Vertex> getVerticesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    @Override
    public void alterElementVisibility(StorableElement element, Visibility newVisibility, Authorizations authorizations) {
        String elementRowKey = element.getId();
        if (element instanceof Edge) {
            Edge edge = (Edge) element;

            String vertexOutRowKey = edge.getVertexId(Direction.OUT);
            StoreMutation vertexOutMutation = new StoreMutation(vertexOutRowKey);
            if (elementMutationBuilder.alterEdgeVertexOutVertex(vertexOutMutation, edge, newVisibility)) {
                addMutations(GeObjectType.VERTEX, vertexOutMutation);
            }

            String vertexInRowKey = edge.getVertexId(Direction.IN);
            StoreMutation vertexInMutation = new StoreMutation(vertexInRowKey);
            if (elementMutationBuilder.alterEdgeVertexInVertex(vertexInMutation, edge, newVisibility)) {
                addMutations(GeObjectType.VERTEX, vertexInMutation);
            }
        }

        StoreMutation m = new StoreMutation(elementRowKey);
        if (elementMutationBuilder.alterElementVisibility(m, element, newVisibility)) {
            addMutations(element, m);
        }
        element.setVisibility(newVisibility);
    }

    @Override
    public void alterEdgeLabel(E edge, String newEdgeLabel, Authorizations authorizations) {
        elementMutationBuilder.alterEdgeLabel(edge, newEdgeLabel);
        edge.setLabel(newEdgeLabel);
    }

    @Override
    public void alterConceptType(V vertex, String newConceptType) {
        elementMutationBuilder.alterVertexConceptType(vertex, newConceptType);
        vertex.setConceptType(newConceptType);
    }

    @Override
    public void alterElementPropertyVisibilities(StorableElement element, List<AlterPropertyVisibility> alterPropertyVisibilities) {
        if (alterPropertyVisibilities.size() == 0) {
            return;
        }

        String elementRowKey = element.getId();

        StoreMutation m = new StoreMutation(elementRowKey);

        List<PropertyDescriptor> propertyList = Lists.newArrayList();
        for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
            MutableProperty property = (MutableProperty) element.getProperty(
                    apv.getKey(),
                    apv.getName(),
                    apv.getExistingVisibility()
            );
            if (property == null) {
                throw new GeException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            if (property.getVisibility().equals(apv.getVisibility())) {
                continue;
            }
            if (apv.getExistingVisibility() == null) {
                apv.setExistingVisibility(property.getVisibility());
            }
            elementMutationBuilder.addPropertySoftDeleteToMutation(m, property);
            property.setVisibility(apv.getVisibility());
            property.setTimestamp(apv.getTimestamp());
            elementMutationBuilder.addPropertyToMutation(this, m, elementRowKey, property);

            // Keep track of properties that need to be removed from indices
            propertyList.add(PropertyDescriptor.from(apv.getKey(), apv.getName(), apv.getExistingVisibility()));
        }


        if (!propertyList.isEmpty()) {
            addMutations(element, m);
        }
    }

    @Override
    public void alterPropertyMetadatas(StorableElement element, List<SetPropertyMetadata> setPropertyMetadatas) {
        if (setPropertyMetadatas.size() == 0) {
            return;
        }

        String elementRowKey = element.getId();
        StoreMutation m = new StoreMutation(elementRowKey);
        for (SetPropertyMetadata apm : setPropertyMetadatas) {
            Property property = element.getProperty(apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility());
            if (property == null) {
                throw new GeException(String.format("Could not find property %s:%s(%s)", apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility()));
            }
            if (property.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
                property.getMetadata().add(apm.getMetadataName(), apm.getNewValue(), apm.getMetadataVisibility());
            }
            elementMutationBuilder.addPropertyMetadataItemToMutation(
                    m,
                    property,
                    apm.getMetadataName(),
                    apm.getNewValue(),
                    apm.getMetadataVisibility()
            );
        }

        addMutations(element, m);
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        return authorizations.canRead(visibility);
    }


    @Override
    public void markPropertyHidden(
            StorableElement element,
            Property property,
            Long timestamp,
            Visibility visibility,
            @SuppressWarnings("UnusedParameters") Authorizations authorizations
    ) {
        checkNotNull(element);
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        ColumnVisibility columnVisibility = ElementMutationBuilder.visibilityToColumnVisibility(visibility);

        if (element instanceof Vertex) {
            addMutations(GeObjectType.VERTEX, getMarkHiddenPropertyMutation(element.getId(), property, timestamp, columnVisibility));
        } else if (element instanceof Edge) {
            addMutations(GeObjectType.EDGE, getMarkHiddenPropertyMutation(element.getId(), property, timestamp, columnVisibility));
        }

        getSearchIndex().markPropertyHidden(this, element, property, visibility, authorizations);

        if (hasEventListeners()) {
            queueEvent(new MarkHiddenPropertyEvent(this, element, property, visibility));
        }
    }

    private StoreMutation getMarkHiddenPropertyMutation(String rowKey, Property property, long timestamp, ColumnVisibility visibility) {
        StoreMutation m = new StoreMutation(rowKey);
        String columnQualifier = StorableKeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(property, getNameSubstitutionStrategy());
        m.put(StorableElement.CF_PROPERTY_HIDDEN, columnQualifier, visibility, timestamp, StorableElement.HIDDEN_VALUE);
        return m;
    }

    @Override
    public void markPropertyVisible(StorableElement element, Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        checkNotNull(element);
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        ColumnVisibility columnVisibility = ElementMutationBuilder.visibilityToColumnVisibility(visibility);

        if (element instanceof Vertex) {
            addMutations(GeObjectType.VERTEX, getMarkVisiblePropertyMutation(element.getId(), property, timestamp, columnVisibility));
        } else if (element instanceof Edge) {
            addMutations(GeObjectType.EDGE, getMarkVisiblePropertyMutation(element.getId(), property, timestamp, columnVisibility));
        }

        getSearchIndex().markPropertyVisible(this, element, property, visibility, authorizations);

        if (hasEventListeners()) {
            queueEvent(new MarkVisiblePropertyEvent(this, element, property, visibility));
        }
    }

    private StoreMutation getMarkVisiblePropertyMutation(String rowKey, Property property, long timestamp, ColumnVisibility visibility) {
        StoreMutation m = new StoreMutation(rowKey);
        String columnQualifier = StorableKeyHelper.getColumnQualifierFromPropertyHiddenColumnQualifier(property, getNameSubstitutionStrategy());
        m.put(StorableElement.CF_PROPERTY_HIDDEN, columnQualifier, visibility, timestamp, StorableElement.HIDDEN_VALUE_DELETED);
        return m;
    }

    @Override
    public long getVertexCount(Authorizations authorizations) {
        String tableName = getTableNameFromElementType(ElementType.VERTEX);
        return getRowCountFromTable(tableName, StorableVertex.CF_SIGNAL, authorizations);
    }

    @Override
    public long getEdgeCount(Authorizations authorizations) {
        String tableName = getTableNameFromElementType(ElementType.EDGE);
        return getRowCountFromTable(tableName, StorableEdge.CF_SIGNAL, authorizations);
    }

    protected abstract long getRowCountFromTable(String tableName, String signalColumn, Authorizations authorizations);

    protected static abstract class AddEdgeToVertexRunnable {
        public abstract void run(StorableEdge edge);
    }

    private StoreMutation[] getDeleteExtendedDataMutations(ExtendedDataRowId rowId) {
        StoreMutation[] mutations = new StoreMutation[1];
        String rowKey = StorableKeyHelper.createExtendedDataRowKey(rowId);
        StoreMutation m = new StoreMutation(rowKey);
        m.put(StorableElement.DELETE_ROW_COLUMN_FAMILY, StorableElement.DELETE_ROW_COLUMN_QUALIFIER, ElementMutationBuilder.DELETE_ROW_VALUE);
        mutations[0] = m;
        return mutations;
    }

    public void invalidateElementFromCache(ElementType elementType, String id) {
        if (cacheEnabled) {
            elementCacheService.invalidate(ElementType.VERTEX.equals(elementType) ? VERTEX_CACHE_NAME : EDGE_CACHE_NAME, id);
        }
    }

    public void addElementToCache(ElementType elementType, Element element) {
        if (cacheEnabled) {
            elementCacheService.put(ElementType.VERTEX.equals(elementType) ? VERTEX_CACHE_NAME : EDGE_CACHE_NAME, element.getId(), element, elementCacheOptions);
        } else {
            LOGGER.warn("Element caching is not enabled");
        }
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        return graphMetadataStore;
    }

    public void setGraphMetadataStore(GraphMetadataStore graphMetadataStore) {
        this.graphMetadataStore = graphMetadataStore;
    }

    private void queueEvent(GraphEvent graphEvent) {
        if (!hasEventListeners()) {
            return;
        }
        synchronized (this.graphEventQueue) {
            this.graphEventQueue.add(graphEvent);
        }
    }

    public static String getVerticesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_v";
    }

    public static String getHistoryVerticesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_vh";
    }

    public static String getEdgesTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_e");
    }

    public static String getHistoryEdgesTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_eh");
    }

    public static String getExtendedDataTableName(String tableNamePrefix) {
        return tableNamePrefix + "_extdata";
    }

    public static String getDataTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_d");
    }

    public static String getMetadataTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_m");
    }

    public String getVerticesTableName() {
        return verticesTableName;
    }

    public String getHistoryVerticesTableName() {
        return historyVerticesTableName;
    }

    public String getEdgesTableName() {
        return edgesTableName;
    }

    public String getHistoryEdgesTableName() {
        return historyEdgesTableName;
    }

    public static String getExtendedDataTableName() {
        return extendedDataTableName;
    }

    public String getDataTableName() {
        return dataTableName;
    }

    public String getMetadataTableName() {
        return metadataTableName;
    }

    protected boolean isHistoryInSeparateTable() {
        return historyInSeparateTable;
    }

    public String getTableNameFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return getVerticesTableName();
            case EDGE:
                return getEdgesTableName();
            default:
                throw new GeException("Unexpected element type: " + elementType);
        }
    }

    public String getHistoryTableNameFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return getHistoryVerticesTableName();
            case EDGE:
                return getHistoryEdgesTableName();
            default:
                throw new GeException("Unexpected element type: " + elementType);
        }
    }


    public StreamingPropertyValueStorageStrategy getStreamingPropertyValueStorageStrategy() {
        return streamingPropertyValueStorageStrategy;
    }

    public NameSubstitutionStrategy getNameSubstitutionStrategy() {
        return nameSubstitutionStrategy;
    }

    public GeSerializer getGeSerializer() {
        return geSerializer;
    }

    private class DeleteElementsConsumer implements Consumer<ElementId> {
        private final Authorizations authorizations;
        private final Set<String> verticesToFetch = new HashSet<>();
        private final Set<String> edgesToFetch = new HashSet<>();
        private final Set<Vertex> verticesToDelete = new HashSet<>();
        private final Set<EdgeElementLocation> edgesToDelete = new HashSet<>();

        public DeleteElementsConsumer(Authorizations authorizations) {
            this.authorizations = authorizations;
        }

        @Override
        public void accept(ElementId elementId) {
            if (elementId instanceof Vertex) {
                verticesToDelete.add((Vertex) elementId);
            } else if (elementId instanceof EdgeElementLocation) {
                edgesToDelete.add((EdgeElementLocation) elementId);
            } else if (elementId.getElementType() == ElementType.VERTEX) {
                verticesToFetch.add(elementId.getId());
            } else if (elementId.getElementType() == ElementType.EDGE) {
                edgesToFetch.add(elementId.getId());
            } else {
                throw new GeException("unhandled element type: " + elementId.getElementType());
            }
            processBatches(false);
        }

        public void processBatches(boolean finalBatch) {
            if (finalBatch || verticesToFetch.size() > 100) {
                verticesToDelete.addAll(toList(getVertices(verticesToFetch, FetchHints.EDGE_REFS, authorizations)));
                verticesToFetch.clear();
            }

            if (finalBatch || verticesToDelete.size() > 100) {
                for (Vertex vertexToDelete : verticesToDelete) {
                    edgesToFetch.addAll(toList(vertexToDelete.getEdgeIds(Direction.BOTH, authorizations)));
                }
                deleteVertices(verticesToDelete);
                verticesToDelete.clear();
            }

            if (finalBatch || edgesToFetch.size() > 100) {
                edgesToDelete.addAll(toList(getEdges(edgesToFetch, FetchHints.NONE, authorizations)));
                edgesToFetch.clear();
            }

            if (finalBatch || edgesToDelete.size() > 100) {
                deleteEdges(edgesToDelete);
                edgesToDelete.clear();
            }
        }

        private void deleteVertices(Set<Vertex> verticesToDelete) {
            getSearchIndex().deleteElements(AbstractStorableGraph.this, verticesToDelete, authorizations);

            deleteAllExtendedDataForElements(verticesToDelete, authorizations);

            for (Vertex vertex : verticesToDelete) {
                addMutations(GeObjectType.VERTEX, elementMutationBuilder.getDeleteRowMutation(vertex.getId()));
                queueEvent(new DeleteVertexEvent(AbstractStorableGraph.this, vertex));
                if (cacheEnabled) {
                    elementCacheService.invalidate(VERTEX_CACHE_NAME, vertex.getId());
                }
            }
        }

        private void deleteEdges(Set<EdgeElementLocation> edgesToDelete) {
            getSearchIndex().deleteElements(AbstractStorableGraph.this, edgesToDelete, authorizations);

            deleteAllExtendedDataForElements(edgesToDelete, authorizations);

            for (EdgeElementLocation edgeLocation : edgesToDelete) {
                ColumnVisibility visibility = ElementMutationBuilder.visibilityToColumnVisibility(edgeLocation.getVisibility());

                StoreMutation outMutation = new StoreMutation(edgeLocation.getVertexId(Direction.OUT));
                outMutation.putDelete(StorableVertex.CF_OUT_EDGE, edgeLocation.getId(), visibility);

                StoreMutation inMutation = new StoreMutation(edgeLocation.getVertexId(Direction.IN));
                inMutation.putDelete(StorableVertex.CF_IN_EDGE, edgeLocation.getId(), visibility);

                addMutations(GeObjectType.VERTEX, outMutation, inMutation);

                // Deletes everything else related to edge.
                addMutations(GeObjectType.EDGE, elementMutationBuilder.getDeleteRowMutation(edgeLocation.getId()));

                queueEvent(new DeleteEdgeEvent(AbstractStorableGraph.this, edgeLocation));

                if (cacheEnabled) {
                    elementCacheService.invalidate(EDGE_CACHE_NAME, edgeLocation.getId());
                    elementCacheService.invalidate(VERTEX_CACHE_NAME, edgeLocation.getVertexId(Direction.OUT));
                    elementCacheService.invalidate(VERTEX_CACHE_NAME, edgeLocation.getVertexId(Direction.IN));
                }
            }
        }

        private void deleteAllExtendedDataForElements(Iterable<? extends ElementId> elementIds, Authorizations authorizations) {
            FetchHints fetchHints = new FetchHintsBuilder()
                    .setIncludeExtendedDataTableNames(true)
                    .build();
            Iterable<ExtendedDataRow> rows = getExtendedDataForElements(
                    elementIds,
                    fetchHints,
                    authorizations
            );
            for (ExtendedDataRow row : rows) {
                deleteExtendedDataRow(row.getId(), authorizations);
            }
        }
    }
}
