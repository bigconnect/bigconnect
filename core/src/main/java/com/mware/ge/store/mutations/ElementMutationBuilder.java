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
package com.mware.ge.store.mutations;

import com.mware.ge.*;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.mutation.*;
import com.mware.ge.security.ColumnVisibility;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.store.*;
import com.mware.ge.store.util.StreamingPropertyValueStorageStrategy;
import com.mware.ge.util.ExtendedDataMutationUtils;
import com.mware.ge.util.IncreasingTime;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValueRef;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static com.mware.ge.store.util.StorableKeyHelper.*;
import static com.mware.ge.util.IncreasingTime.currentTimeMillis;

public abstract class ElementMutationBuilder {
    public static final byte[] EMPTY_VALUE = "".getBytes();
    public static final byte[] DELETE_ROW_VALUE = "DEL_ROW".getBytes(StandardCharsets.UTF_8);

    protected final StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy;
    protected final GeSerializer geSerializer;
    protected final StorableGraph graph;

    protected static final Cache<String, String> propertyMetadataColumnQualifierTextCache = Cache2kBuilder
            .of(String.class, String.class)
            .entryCapacity(10000)
            .name(ElementMutationBuilder.class, "propertyMetadataColumnQualifierTextCache")
            .eternal(true)
            .build();

    protected ElementMutationBuilder(
            StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy,
            StorableGraph graph,
            GeSerializer geSerializer
    ) {
        this.streamingPropertyValueStorageStrategy = streamingPropertyValueStorageStrategy;
        this.graph = graph;
        this.geSerializer = geSerializer;
    }

    public void saveVertexBuilder(StorableGraph graph, VertexBuilder vertexBuilder, long timestamp) {
        StoreMutation m = createMutationForVertexBuilder(graph, vertexBuilder, timestamp);
        saveVertexMutation(m);
        saveExtendedDataMutations(graph, ElementType.VERTEX, vertexBuilder);
    }

    private <T extends Element> void saveExtendedDataMutations(StorableGraph graph, ElementType elementType, ElementBuilder<T> elementBuilder) {
        saveExtendedData(
                graph,
                elementBuilder.getId(),
                elementType,
                elementBuilder.getExtendedData(),
                elementBuilder.getExtendedDataDeletes()
        );
    }

    public void saveExtendedData(
            StorableGraph graph,
            String elementId,
            ElementType elementType,
            Iterable<ExtendedDataMutation> extendedData,
            Iterable<ExtendedDataDeleteMutation> extendedDataDeletes
    ) {
        Map<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowId = ExtendedDataMutationUtils.getByTableThenRowId(
                extendedData,
                extendedDataDeletes
        );

        for (Map.Entry<String, Map<String, ExtendedDataMutationUtils.Mutations>> byTableThenRowIdEntry : byTableThenRowId.entrySet()) {
            String tableName = byTableThenRowIdEntry.getKey();
            Map<String, ExtendedDataMutationUtils.Mutations> byRowId = byTableThenRowIdEntry.getValue();
            for (Map.Entry<String, ExtendedDataMutationUtils.Mutations> byRowIdEntry : byRowId.entrySet()) {
                String row = byRowIdEntry.getKey();
                ExtendedDataMutationUtils.Mutations mutations = byRowIdEntry.getValue();

                StoreMutation m = new StoreMutation(createExtendedDataRowKey(elementType, elementId, tableName, row));

                for (ExtendedDataMutation edm : mutations.getExtendedData()) {
                    com.mware.ge.values.storable.Value value = transformValue(edm.getValue(), null, null);

                    // graph can be null if this is running in Map Reduce. We can just assume the property is already defined.
                    if (graph != null) {
                        graph.ensurePropertyDefined(edm.getColumnName(), value);
                    }

                    m.put(
                            StorableElement.CF_EXTENDED_DATA,
                            createExtendedDataColumnQualifier(edm),
                            visibilityToColumnVisibility(edm.getVisibility()),
                            geSerializer.objectToBytes(value)
                    );
                }

                for (ExtendedDataDeleteMutation eddm : mutations.getExtendedDataDeletes()) {
                    m.putDelete(
                            StorableElement.CF_EXTENDED_DATA,
                            createExtendedDataColumnQualifier(eddm),
                            visibilityToColumnVisibility(eddm.getVisibility())
                    );
                }

                saveExtendedDataMutation(elementType, m);
            }
        }
    }

    protected abstract void saveExtendedDataMutation(ElementType elementType, StoreMutation m);

    protected abstract void saveVertexMutation(StoreMutation m);

    private StoreMutation createMutationForVertexBuilder(StorableGraph graph, VertexBuilder vertexBuilder, Long timestamp) {
        String vertexRowKey = vertexBuilder.getId();
        StoreMutation m = new StoreMutation(vertexRowKey);
        ColumnVisibility visibility = visibilityToColumnVisibility(vertexBuilder.getVisibility());

        String conceptType = vertexBuilder.getConceptType();
        if (vertexBuilder.getNewConceptType() != null && !vertexBuilder.getNewConceptType().equals(conceptType)) {
            conceptType = vertexBuilder.getNewConceptType();
            m.putDelete(StorableVertex.CF_SIGNAL, vertexBuilder.getConceptType(), visibility, currentTimeMillis());
        }

        if (timestamp != null)
            m.put(StorableVertex.CF_SIGNAL, conceptType, visibility, timestamp, EMPTY_VALUE);
        else
            m.put(StorableVertex.CF_SIGNAL, conceptType, visibility, EMPTY_VALUE);

        createMutationForElementBuilder(graph, vertexBuilder, vertexRowKey, m);
        return m;
    }

    private <T extends Element> void createMutationForElementBuilder(
            StorableGraph graph,
            ElementBuilder<T> elementBuilder,
            String rowKey,
            StoreMutation m
    ) {
        for (PropertyDeleteMutation propertyDeleteMutation : elementBuilder.getPropertyDeletes()) {
            addPropertyDeleteToMutation(m, propertyDeleteMutation);
        }
        for (PropertySoftDeleteMutation propertySoftDeleteMutation : elementBuilder.getPropertySoftDeletes()) {
            addPropertySoftDeleteToMutation(m, propertySoftDeleteMutation);
        }
        for (Property property : elementBuilder.getProperties()) {
            addPropertyToMutation(graph, m, rowKey, property);
        }
        Iterable<ExtendedDataMutation> extendedData = elementBuilder.getExtendedData();
        saveExtendedDataMarkers(m, extendedData);
    }

    public void saveExtendedDataMarkers(
            String elementId,
            ElementType elementType,
            Iterable<ExtendedDataMutation> extendedData
    ) {
        Set<TableNameVisibilityPair> uniquePairs = TableNameVisibilityPair.getUniquePairs(extendedData);
        if (uniquePairs.size() == 0) {
            return;
        }
        StoreMutation m = new StoreMutation(elementId);
        for (TableNameVisibilityPair pair : uniquePairs) {
            addExtendedDataMarkerToElementMutation(m, pair);
        }
        saveElementMutation(elementType, m);
    }

    private void saveElementMutation(ElementType elementType, StoreMutation m) {
        switch (elementType) {
            case VERTEX:
                saveVertexMutation(m);
                break;
            case EDGE:
                saveEdgeMutation(m);
                break;
            default:
                throw new GeException("Unhandled element type: " + elementType);
        }
    }

    private void saveExtendedDataMarkers(StoreMutation m, Iterable<ExtendedDataMutation> extendedData) {
        for (TableNameVisibilityPair pair : TableNameVisibilityPair.getUniquePairs(extendedData)) {
            addExtendedDataMarkerToElementMutation(m, pair);
        }
    }

    private void addExtendedDataMarkerToElementMutation(StoreMutation m, TableNameVisibilityPair pair) {
        m.put(
                StorableElement.CF_EXTENDED_DATA,
                pair.getTableName(),
                visibilityToColumnVisibility(pair.getVisibility()),
                pair.getTableName().getBytes()
        );
    }

    private com.mware.ge.values.storable.Value transformValue(com.mware.ge.values.storable.Value propertyValue, String rowKey, Property property) {
        if (propertyValue instanceof StreamingPropertyValue) {
            if (rowKey != null && property != null) {
                propertyValue = saveStreamingPropertyValue(rowKey, property, (StreamingPropertyValue) propertyValue);
            } else {
                throw new GeException(StreamingPropertyValue.class.getSimpleName() + " are not supported");
            }
        }
        return propertyValue;
    }

    public void saveEdgeBuilder(StorableGraph graph, EdgeBuilderBase edgeBuilder, long timestamp) {
        ColumnVisibility edgeColumnVisibility = visibilityToColumnVisibility(edgeBuilder.getVisibility());
        StoreMutation m = createMutationForEdgeBuilder(graph, edgeBuilder, edgeColumnVisibility, timestamp);
        saveEdgeMutation(m);

        String edgeLabel = edgeBuilder.getNewEdgeLabel() != null ? edgeBuilder.getNewEdgeLabel() : edgeBuilder.getEdgeLabel();
        saveEdgeInfoOnVertex(
                edgeBuilder.getId(),
                edgeBuilder.getVertexId(Direction.OUT),
                edgeBuilder.getVertexId(Direction.IN),
                edgeLabel,
                edgeColumnVisibility
        );

        saveExtendedDataMutations(graph, ElementType.EDGE, edgeBuilder);
    }

    private void saveEdgeInfoOnVertex(String edgeId, String outVertexId, String inVertexId, String edgeLabel, ColumnVisibility edgeColumnVisibility) {
        // Update out vertex.
        StoreMutation addEdgeToOutMutation = new StoreMutation(outVertexId);
        StorableEdgeInfo edgeInfo = new StorableEdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), inVertexId);
        addEdgeToOutMutation.put(StorableVertex.CF_OUT_EDGE, edgeId, edgeColumnVisibility, edgeInfo.getBytes());
        saveVertexMutation(addEdgeToOutMutation);

        // Update in vertex.
        StoreMutation addEdgeToInMutation = new StoreMutation(inVertexId);
        edgeInfo = new StorableEdgeInfo(getNameSubstitutionStrategy().deflate(edgeLabel), outVertexId);
        addEdgeToInMutation.put(StorableVertex.CF_IN_EDGE, edgeId, edgeColumnVisibility, edgeInfo.getBytes());
        saveVertexMutation(addEdgeToInMutation);
    }

    public void alterEdgeLabel(Edge edge, String newEdgeLabel) {
        ColumnVisibility edgeColumnVisibility = visibilityToColumnVisibility(edge.getVisibility());
        StoreMutation m = createAlterEdgeLabelMutation(edge, newEdgeLabel, edgeColumnVisibility);
        saveEdgeMutation(m);

        saveEdgeInfoOnVertex(
                edge.getId(),
                edge.getVertexId(Direction.OUT),
                edge.getVertexId(Direction.IN),
                newEdgeLabel,
                edgeColumnVisibility
        );
    }

    public void alterVertexConceptType(Vertex vertex, String newConceptType) {
        ColumnVisibility vertexColumnVisibility = visibilityToColumnVisibility(vertex.getVisibility());
        StoreMutation m = createAlterVertexConceptTypeMutation(vertex, newConceptType, vertexColumnVisibility);
        saveVertexMutation(m);
    }

    public static ColumnVisibility visibilityToColumnVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    protected abstract void saveEdgeMutation(StoreMutation m);

    private StoreMutation createMutationForEdgeBuilder(
            StorableGraph graph,
            EdgeBuilderBase edgeBuilder,
            ColumnVisibility edgeColumnVisibility,
            Long timestamp
    ) {
        String edgeRowKey = edgeBuilder.getId();
        StoreMutation m = new StoreMutation(edgeRowKey);
        String edgeLabel = edgeBuilder.getEdgeLabel();
        if (edgeBuilder.getNewEdgeLabel() != null && !edgeBuilder.getNewEdgeLabel().equals(edgeLabel)) {
            edgeLabel = edgeBuilder.getNewEdgeLabel();
            m.putDelete(StorableEdge.CF_SIGNAL, edgeBuilder.getEdgeLabel(), edgeColumnVisibility, currentTimeMillis());
        }

        if (timestamp != null) {
            m.put(StorableEdge.CF_SIGNAL, edgeLabel, edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
            m.put(StorableEdge.CF_OUT_VERTEX, edgeBuilder.getVertexId(Direction.OUT), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
            m.put(StorableEdge.CF_IN_VERTEX, edgeBuilder.getVertexId(Direction.IN), edgeColumnVisibility, timestamp, ElementMutationBuilder.EMPTY_VALUE);
        } else {
            m.put(StorableEdge.CF_SIGNAL, edgeLabel, edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
            m.put(StorableEdge.CF_OUT_VERTEX, edgeBuilder.getVertexId(Direction.OUT), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
            m.put(StorableEdge.CF_IN_VERTEX, edgeBuilder.getVertexId(Direction.IN), edgeColumnVisibility, ElementMutationBuilder.EMPTY_VALUE);
        }

        createMutationForElementBuilder(graph, edgeBuilder, edgeRowKey, m);
        return m;
    }

    private StoreMutation createAlterEdgeLabelMutation(Edge edge, String newEdgeLabel, ColumnVisibility edgeColumnVisibility) {
        String edgeRowKey = edge.getId();
        StoreMutation m = new StoreMutation(edgeRowKey);
        m.putDelete(StorableEdge.CF_SIGNAL, edge.getLabel(), edgeColumnVisibility, currentTimeMillis());
        m.put(StorableEdge.CF_SIGNAL, newEdgeLabel, edgeColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        return m;
    }

    private StoreMutation createAlterVertexConceptTypeMutation(Vertex vertex, String newConceptType, ColumnVisibility columnVisibility) {
        String vertexRowKey = vertex.getId();
        StoreMutation m = new StoreMutation(vertexRowKey);
        m.putDelete(StorableVertex.CF_SIGNAL, vertex.getConceptType(), columnVisibility, currentTimeMillis());
        m.put(StorableVertex.CF_SIGNAL, newConceptType, columnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        return m;
    }

    public boolean alterElementVisibility(StoreMutation m, StorableElement element, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToColumnVisibility(element.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToColumnVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }

        if (element instanceof StorableEdge) {
            StorableEdge edge = (StorableEdge) element;
            m.putDelete(StorableEdge.CF_SIGNAL, edge.getLabel(), currentColumnVisibility, currentTimeMillis());
            m.put(StorableEdge.CF_SIGNAL, edge.getLabel(), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(StorableEdge.CF_OUT_VERTEX, edge.getVertexId(Direction.OUT), currentColumnVisibility, currentTimeMillis());
            m.put(StorableEdge.CF_OUT_VERTEX, edge.getVertexId(Direction.OUT), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);

            m.putDelete(StorableEdge.CF_IN_VERTEX, edge.getVertexId(Direction.IN), currentColumnVisibility, currentTimeMillis());
            m.put(StorableEdge.CF_IN_VERTEX, edge.getVertexId(Direction.IN), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        } else if (element instanceof StorableVertex) {
            StorableVertex vertex = (StorableVertex) element;
            m.putDelete(StorableVertex.CF_SIGNAL, vertex.getConceptType(), currentColumnVisibility, currentTimeMillis());
            m.put(StorableVertex.CF_SIGNAL, vertex.getConceptType(), newColumnVisibility, currentTimeMillis(), ElementMutationBuilder.EMPTY_VALUE);
        } else {
            throw new IllegalArgumentException("Invalid element type: " + element);
        }
        return true;
    }

    public boolean alterEdgeVertexOutVertex(StoreMutation vertexOutMutation, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToColumnVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToColumnVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        StorableEdgeInfo edgeInfo = new StorableEdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.IN));
        vertexOutMutation.putDelete(StorableVertex.CF_OUT_EDGE, edge.getId(), currentColumnVisibility);
        vertexOutMutation.put(StorableVertex.CF_OUT_EDGE, edge.getId(), newColumnVisibility, edgeInfo.getBytes());
        return true;
    }

    public boolean alterEdgeVertexInVertex(StoreMutation vertexInMutation, Edge edge, Visibility newVisibility) {
        ColumnVisibility currentColumnVisibility = visibilityToColumnVisibility(edge.getVisibility());
        ColumnVisibility newColumnVisibility = visibilityToColumnVisibility(newVisibility);
        if (currentColumnVisibility.equals(newColumnVisibility)) {
            return false;
        }
        StorableEdgeInfo edgeInfo = new StorableEdgeInfo(getNameSubstitutionStrategy().deflate(edge.getLabel()), edge.getVertexId(Direction.OUT));
        vertexInMutation.putDelete(StorableVertex.CF_IN_EDGE, edge.getId(), currentColumnVisibility);
        vertexInMutation.put(StorableVertex.CF_IN_EDGE, edge.getId(), newColumnVisibility, edgeInfo.getBytes());
        return true;
    }

    public void addPropertyToMutation(StorableGraph graph, StoreMutation m, String rowKey, Property property) {
        String columnQualifier = getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToColumnVisibility(property.getVisibility());
        com.mware.ge.values.storable.Value propertyValue = transformValue(property.getValue(), rowKey, property);

        // graph can be null if this is running in Map Reduce. We can just assume the property is already defined.
        if (graph != null) {
            graph.ensurePropertyDefined(property.getName(), propertyValue);
        }

        byte[] value = geSerializer.objectToBytes(propertyValue);
        long ts = property.getTimestamp() == null ? IncreasingTime.currentTimeMillis() : property.getTimestamp();
        m.put(StorableElement.CF_PROPERTY, columnQualifier, columnVisibility, ts, value);
        addPropertyMetadataToMutation(m, property);
    }

    protected abstract NameSubstitutionStrategy getNameSubstitutionStrategy();

    public void addPropertyDeleteToMutation(StoreMutation m, PropertyDeleteMutation propertyDelete) {
        String columnQualifier = getColumnQualifierFromPropertyColumnQualifier(
                propertyDelete.getKey(),
                propertyDelete.getName(),
                getNameSubstitutionStrategy()
        );
        ColumnVisibility columnVisibility = visibilityToColumnVisibility(propertyDelete.getVisibility());
        m.putDelete(StorableElement.CF_PROPERTY, columnQualifier, columnVisibility, currentTimeMillis());
        addPropertyDeleteMetadataToMutation(m, propertyDelete);
    }

    public void addPropertyMetadataToMutation(StoreMutation m, Property property) {
        Metadata metadata = property.getMetadata();
        for (Metadata.Entry metadataItem : metadata.entrySet()) {
            addPropertyMetadataItemToMutation(
                    m,
                    property,
                    metadataItem.getKey(),
                    metadataItem.getValue(),
                    metadataItem.getVisibility()
            );
        }
    }

    public void addPropertyMetadataItemToMutation(
            StoreMutation m,
            Property property,
            String metadataKey,
            Object metadataValue,
            Visibility visibility
    ) {
        String columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataKey);
        ColumnVisibility metadataVisibility = visibilityToColumnVisibility(visibility);
        if (metadataValue == null) {
            addPropertyMetadataItemDeleteToMutation(m, columnQualifier, metadataVisibility);
        } else {
            addPropertyMetadataItemAddToMutation(m, columnQualifier, metadataVisibility, property.getTimestamp(), metadataValue);
        }
    }

    private void addPropertyMetadataItemAddToMutation(StoreMutation m, String columnQualifier, ColumnVisibility metadataVisibility, Long propertyTimestamp, Object value) {
        byte[] metadataValue = geSerializer.objectToBytes(value);
        if (propertyTimestamp != null)
            m.put(StorableElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, propertyTimestamp, metadataValue);
        else
            m.put(StorableElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, metadataValue);
    }

    private void addPropertyMetadataItemDeleteToMutation(StoreMutation m, String columnQualifier, ColumnVisibility metadataVisibility) {
        m.putDelete(StorableElement.CF_PROPERTY_METADATA, columnQualifier, metadataVisibility, currentTimeMillis());
    }

    private String getPropertyMetadataColumnQualifierText(Property property, String metadataKey) {
        String propertyName = property.getName();
        String propertyKey = property.getKey();
        String visibilityString = property.getVisibility().getVisibilityString();
        //noinspection StringBufferReplaceableByString - for speed we use StringBuilder
        StringBuilder keyBuilder = new StringBuilder(propertyName.length() + propertyKey.length() + visibilityString.length() + metadataKey.length());
        keyBuilder.append(getNameSubstitutionStrategy().deflate(propertyName));
        keyBuilder.append(getNameSubstitutionStrategy().deflate(propertyKey));
        keyBuilder.append(visibilityString);
        keyBuilder.append(getNameSubstitutionStrategy().deflate(metadataKey));
        String key = keyBuilder.toString();
        String r = propertyMetadataColumnQualifierTextCache.peek(key);
        if (r == null) {
            r = getColumnQualifierFromPropertyMetadataColumnQualifier(
                    propertyName,
                    propertyKey,
                    visibilityString,
                    metadataKey,
                    getNameSubstitutionStrategy()
            );
            propertyMetadataColumnQualifierTextCache.put(key, r);
        }
        return r;
    }

    public void addPropertyDeleteMetadataToMutation(StoreMutation m, PropertyDeleteMutation propertyDeleteMutation) {
        if (propertyDeleteMutation instanceof PropertyPropertyDeleteMutation) {
            Property property = ((PropertyPropertyDeleteMutation) propertyDeleteMutation).getProperty();
            Metadata metadata = property.getMetadata();
            for (Metadata.Entry metadataItem : metadata.entrySet()) {
                String columnQualifier = getPropertyMetadataColumnQualifierText(property, metadataItem.getKey());
                ColumnVisibility metadataVisibility = visibilityToColumnVisibility(metadataItem.getVisibility());
                addPropertyMetadataItemDeleteToMutation(m, columnQualifier, metadataVisibility);
            }
        }
    }

    public void addPropertyDeleteToMutation(StoreMutation m, Property property) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
        String columnQualifier = getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToColumnVisibility(property.getVisibility());
        m.putDelete(StorableElement.CF_PROPERTY, columnQualifier, columnVisibility, currentTimeMillis());
        for (Metadata.Entry metadataEntry : property.getMetadata().entrySet()) {
            String metadataEntryColumnQualifier = getPropertyMetadataColumnQualifierText(property, metadataEntry.getKey());
            ColumnVisibility metadataEntryVisibility = visibilityToColumnVisibility(metadataEntry.getVisibility());
            addPropertyMetadataItemDeleteToMutation(m, metadataEntryColumnQualifier, metadataEntryVisibility);
        }
    }

    public void addPropertySoftDeleteToMutation(StoreMutation m, Property property) {
        Preconditions.checkNotNull(m, "mutation cannot be null");
        Preconditions.checkNotNull(property, "property cannot be null");
        String columnQualifier = getColumnQualifierFromPropertyColumnQualifier(property, getNameSubstitutionStrategy());
        ColumnVisibility columnVisibility = visibilityToColumnVisibility(property.getVisibility());
        m.put(StorableElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, currentTimeMillis(), StorableElement.SOFT_DELETE_VALUE);
    }

    public void addPropertySoftDeleteToMutation(StoreMutation m, PropertySoftDeleteMutation propertySoftDelete) {
        String columnQualifier =  getColumnQualifierFromPropertyColumnQualifier(
                propertySoftDelete.getKey(),
                propertySoftDelete.getName(),
                getNameSubstitutionStrategy()
        );
        ColumnVisibility columnVisibility = visibilityToColumnVisibility(propertySoftDelete.getVisibility());
        assert propertySoftDelete.getTimestamp() != null;
        m.put(StorableElement.CF_PROPERTY_SOFT_DELETE, columnQualifier, columnVisibility, propertySoftDelete.getTimestamp(), StorableElement.SOFT_DELETE_VALUE);
    }

    public abstract void saveDataMutation(StoreMutation dataMutation);

    public StoreMutation getDeleteRowMutation(String rowKey) {
        StoreMutation m = new StoreMutation(rowKey);
        m.put(StorableElement.DELETE_ROW_COLUMN_FAMILY, StorableElement.DELETE_ROW_COLUMN_QUALIFIER, DELETE_ROW_VALUE);
        return m;
    }

    protected StreamingPropertyValueRef saveStreamingPropertyValue(final String rowKey, final Property property, StreamingPropertyValue propertyValue) {
        return streamingPropertyValueStorageStrategy.saveStreamingPropertyValue(this, rowKey, property, propertyValue);
    }
}
