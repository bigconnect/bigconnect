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
package com.mware.ge;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mware.ge.event.GraphEvent;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.metric.StackTraceTracker;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.Query;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValueRef;
import com.mware.ge.util.*;
import com.mware.ge.values.storable.Value;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.Preconditions.checkNotNull;
import static com.mware.ge.util.StreamUtils.stream;

public abstract class GraphBase implements Graph {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(GraphBase.class);
    protected static final GeLogger QUERY_LOGGER = GeLoggerFactory.getQueryLogger(Graph.class);
    public static final String METADATA_DEFINE_PROPERTY_PREFIX = "defineProperty.";
    private final List<GraphEventListener> graphEventListeners = new ArrayList<>();
    private Map<String, PropertyDefinition> propertyDefinitionCache = new ConcurrentHashMap<>();
    private final boolean strictTyping;
    private final GeMetricRegistry metricRegistry;
    protected final com.mware.ge.metric.Timer flushTimer;
    protected final StackTraceTracker flushStackTraceTracker;

    protected GraphBase(boolean strictTyping, GeMetricRegistry metricRegistry) {
        this.strictTyping = strictTyping;
        this.metricRegistry = metricRegistry;
        this.flushTimer = metricRegistry.getTimer(Graph.class, "flush", "timer");
        this.flushStackTraceTracker = metricRegistry.getStackTraceTracker(Graph.class, "flush", "stack");
    }

    @Override
    public Iterable<String> filterEdgeIdsByAuthorization(
            Iterable<String> edgeIds,
            final String authorizationToMatch,
            final EnumSet<ElementFilter> filters,
            Authorizations authorizations
    ) {
        FilterIterable<Edge> edges = new FilterIterable<Edge>(getEdges(edgeIds, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                if (filters.contains(ElementFilter.ELEMENT)) {
                    if (edge.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                return isIncludedByAuthorizations(edge, filters, authorizationToMatch);
            }
        };
        return new ConvertingIterable<Edge, String>(edges) {
            @Override
            protected String convert(Edge edge) {
                return edge.getId();
            }
        };
    }

    private boolean isIncludedByAuthorizations(Element element, EnumSet<ElementFilter> filters, String authorizationToMatch) {
        if (filters.contains(ElementFilter.PROPERTY) || filters.contains(ElementFilter.PROPERTY_METADATA)) {
            for (Property property : element.getProperties()) {
                if (filters.contains(ElementFilter.PROPERTY)) {
                    if (property.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                if (filters.contains(ElementFilter.PROPERTY_METADATA)) {
                    for (Metadata.Entry entry : property.getMetadata().entrySet()) {
                        if (entry.getVisibility().hasAuthorization(authorizationToMatch)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public Iterable<String> filterVertexIdsByAuthorization(
            Iterable<String> vertexIds,
            final String authorizationToMatch,
            final EnumSet<ElementFilter> filters,
            Authorizations authorizations
    ) {
        FilterIterable<Vertex> vertices = new FilterIterable<Vertex>(getVertices(vertexIds, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
            @Override
            protected boolean isIncluded(Vertex vertex) {
                if (filters.contains(ElementFilter.ELEMENT)) {
                    if (vertex.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                return isIncludedByAuthorizations(vertex, filters, authorizationToMatch);
            }
        };
        return new ConvertingIterable<Vertex, String>(vertices) {
            @Override
            protected String convert(Vertex vertex) {
                return vertex.getId();
            }
        };
    }

    @Override
    public Iterable<Path> findPaths(FindPathOptions options, Authorizations authorizations) {
        ProgressCallback progressCallback = options.getProgressCallback();
        if (progressCallback == null) {
            progressCallback = new ProgressCallback() {
                @Override
                public void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount) {
                    LOGGER.debug("findPaths progress %d%%: %s", (int) (progressPercent * 100.0), step.formatMessage(edgeIndex, vertexCount));
                }
            };
        }

        FetchHints fetchHints = FetchHints.EDGE_REFS;
        Vertex sourceVertex = getVertex(options.getSourceVertexId(), fetchHints, authorizations);
        if (sourceVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + options.getSourceVertexId());
        }
        Vertex destVertex = getVertex(options.getDestVertexId(), fetchHints, authorizations);
        if (destVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + options.getDestVertexId());
        }

        progressCallback.progress(0, ProgressCallback.Step.FINDING_PATH);

        Set<String> seenVertices = new HashSet<>();
        seenVertices.add(sourceVertex.getId());

        Path startPath = new Path(sourceVertex.getId());

        List<Path> foundPaths = new ArrayList<>();
        if (options.getMaxHops() == 2) {
            findPathsSetIntersection(
                    options,
                    foundPaths,
                    sourceVertex,
                    destVertex,
                    progressCallback,
                    authorizations
            );
        } else {
            findPathsRecursive(
                    options,
                    foundPaths,
                    sourceVertex,
                    destVertex,
                    options.getMaxHops(),
                    seenVertices,
                    startPath,
                    progressCallback,
                    authorizations
            );
        }

        progressCallback.progress(1, ProgressCallback.Step.COMPLETE);
        return foundPaths;
    }

    protected void findPathsSetIntersection(FindPathOptions options, List<Path> foundPaths, Vertex sourceVertex, Vertex destVertex, ProgressCallback progressCallback, Authorizations authorizations) {
        String sourceVertexId = sourceVertex.getId();
        String destVertexId = destVertex.getId();

        progressCallback.progress(0.1, ProgressCallback.Step.SEARCHING_SOURCE_VERTEX_EDGES);
        Set<String> sourceVertexConnectedVertexIds = filterFindPathEdgeInfo(options, sourceVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), authorizations));
        Map<String, Boolean> sourceVerticesExist = doVerticesExist(sourceVertexConnectedVertexIds, authorizations);
        sourceVertexConnectedVertexIds = stream(sourceVerticesExist.keySet())
                .filter(key -> sourceVerticesExist.getOrDefault(key, false))
                .collect(Collectors.toSet());

        progressCallback.progress(0.3, ProgressCallback.Step.SEARCHING_DESTINATION_VERTEX_EDGES);
        Set<String> destVertexConnectedVertexIds = filterFindPathEdgeInfo(options, destVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), authorizations));
        Map<String, Boolean> destVerticesExist = doVerticesExist(destVertexConnectedVertexIds, authorizations);
        destVertexConnectedVertexIds = stream(destVerticesExist.keySet())
                .filter(key -> destVerticesExist.getOrDefault(key, false))
                .collect(Collectors.toSet());

        if (sourceVertexConnectedVertexIds.contains(destVertexId)) {
            foundPaths.add(new Path(sourceVertexId, destVertexId));
            if (options.isGetAnyPath()) {
                return;
            }
        }

        progressCallback.progress(0.6, ProgressCallback.Step.MERGING_EDGES);
        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        for (String connectedVertexId : sourceVertexConnectedVertexIds) {
            foundPaths.add(new Path(sourceVertexId, connectedVertexId, destVertexId));
        }
    }

    private Set<String> filterFindPathEdgeInfo(FindPathOptions options, Iterable<EdgeInfo> edgeInfos) {
        return stream(edgeInfos)
                .filter(edgeInfo -> {
                    if (options.getExcludedLabels() != null) {
                        return !ArrayUtils.contains(options.getExcludedLabels(), edgeInfo.getLabel());
                    }
                    return true;
                })
                .map(EdgeInfo::getVertexId)
                .collect(Collectors.toSet());
    }

    private Iterable<Vertex> filterFindPathEdgePairs(FindPathOptions options, Iterable<EdgeVertexPair> edgeVertexPairs) {
        return stream(edgeVertexPairs)
                .filter(edgePair -> {
                    if (options.getExcludedLabels() != null) {
                        return !ArrayUtils.contains(options.getExcludedLabels(), edgePair.getEdge().getLabel());
                    }
                    return true;
                })
                .map(EdgeVertexPair::getVertex)
                .collect(Collectors.toList());
    }

    protected void findPathsRecursive(
            FindPathOptions options,
            List<Path> foundPaths,
            Vertex sourceVertex,
            Vertex destVertex,
            int hops,
            Set<String> seenVertices,
            Path currentPath,
            ProgressCallback progressCallback,
            Authorizations authorizations
    ) {
        // if this is our first source vertex report progress back to the progress callback
        boolean firstLevelRecursion = hops == options.getMaxHops();

        if (options.isGetAnyPath() && foundPaths.size() == 1) {
            return;
        }

        seenVertices.add(sourceVertex.getId());
        if (sourceVertex.getId().equals(destVertex.getId())) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            Iterable<Vertex> vertices = filterFindPathEdgePairs(options, sourceVertex.getEdgeVertexPairs(Direction.BOTH, options.getLabels(), authorizations));
            int vertexCount = 0;
            if (firstLevelRecursion) {
                vertices = IterableUtils.toList(vertices);
                vertexCount = ((List<Vertex>) vertices).size();
            }
            int i = 0;
            for (Vertex child : vertices) {
                if (firstLevelRecursion) {
                    // this will never get to 100% since i starts at 0. which is good. 100% signifies done and we still have work to do.
                    double progressPercent = (double) i / (double) vertexCount;
                    progressCallback.progress(progressPercent, ProgressCallback.Step.SEARCHING_EDGES, i + 1, vertexCount);
                }
                if (!seenVertices.contains(child.getId())) {
                    findPathsRecursive(options, foundPaths, child, destVertex, hops - 1, seenVertices, new Path(currentPath, child.getId()), progressCallback, authorizations);
                }
                i++;
            }
        }
        seenVertices.remove(sourceVertex.getId());
    }

    protected abstract GraphMetadataStore getGraphMetadataStore();

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        return getGraphMetadataStore().getMetadata();
    }

    @Override
    public void setMetadata(String key, Object value) {
        getGraphMetadataStore().setMetadata(key, value);
    }

    @Override
    public void removeMetadata(String key) {
        getGraphMetadataStore().removeMetadata(key);
    }

    @Override
    public Object getMetadata(String key) {
        return getGraphMetadataStore().getMetadata(key);
    }

    @Override
    public void reloadMetadata() {
        getGraphMetadataStore().reloadMetadata();
    }

    @Override
    public Iterable<GraphMetadataEntry> getMetadataWithPrefix(String prefix) {
        return getGraphMetadataStore().getMetadataWithPrefix(prefix);
    }

    @Override
    public abstract Query query(GeQueryBuilder queryBuilder, Authorizations authorizations);

    @Override
    public abstract void reindex(Authorizations authorizations);

    @Override
    public abstract void flush();

    @Override
    public abstract void shutdown();

    @Override
    public abstract void drop();

    @Override
    public abstract boolean isFieldBoostSupported();

    @Override
    public abstract SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    @Override
    public void addGraphEventListener(GraphEventListener graphEventListener) {
        this.graphEventListeners.add(graphEventListener);
    }

    @Override
    public void removeGraphEventListener(GraphEventListener graphEventListener) {
        this.graphEventListeners.remove(graphEventListener);
    }

    protected boolean hasEventListeners() {
        return this.graphEventListeners.size() > 0;
    }

    protected void fireGraphEvent(GraphEvent graphEvent) {
        for (GraphEventListener graphEventListener : this.graphEventListeners) {
            graphEventListener.onGraphEvent(graphEvent);
        }
    }

    @Override
    public Authorizations createAuthorizations(Authorizations auths, String... additionalAuthorizations) {
        Set<String> newAuths = new HashSet<>();
        Collections.addAll(newAuths, auths.getAuthorizations());
        Collections.addAll(newAuths, additionalAuthorizations);
        return createAuthorizations(newAuths);
    }

    protected void addToPropertyDefinitionCache(PropertyDefinition propertyDefinition) {
        propertyDefinitionCache.put(propertyDefinition.getPropertyName(), propertyDefinition);
    }

    @VisibleForTesting
    public void clearPropertyDefinitionCache() {
        propertyDefinitionCache.clear();
    }

    public void invalidatePropertyDefinition(String propertyName) {
        PropertyDefinition def = (PropertyDefinition) getMetadata(getPropertyDefinitionKey(propertyName));
        if (def == null) {
            propertyDefinitionCache.remove(propertyName);
        } else if (def != null) {
            addToPropertyDefinitionCache(def);
        }
    }

    public void savePropertyDefinition(PropertyDefinition propertyDefinition) {
        addToPropertyDefinitionCache(propertyDefinition);
        setMetadata(getPropertyDefinitionKey(propertyDefinition.getPropertyName()), propertyDefinition);
    }

    private String getPropertyDefinitionKey(String propertyName) {
        return METADATA_DEFINE_PROPERTY_PREFIX + propertyName;
    }

    @Override
    public PropertyDefinition getPropertyDefinition(String propertyName) {
        return propertyDefinitionCache.getOrDefault(propertyName, null);
    }

    @Override
    public void removePropertyDefinition(String propertyName) {
        PropertyDefinition propertyDefinition = propertyDefinitionCache.get(propertyName);
        if (propertyDefinition != null) {
            getGraphMetadataStore().removeMetadata(getPropertyDefinitionKey(propertyName));
            propertyDefinitionCache.remove(propertyName);
        }
    }

    @Override
    public Collection<PropertyDefinition> getPropertyDefinitions() {
        return propertyDefinitionCache.values();
    }

    @Override
    public boolean isPropertyDefined(String propertyName) {
        return propertyDefinitionCache.containsKey(propertyName);
    }

    public void ensurePropertyDefined(String name, Value value) {
        PropertyDefinition propertyDefinition = getPropertyDefinition(name);
        if (propertyDefinition != null) {
            return;
        }
        Class<? extends Value> valueClass = getValueType(value);
        if (strictTyping) {
            throw new GeTypeException(name, valueClass);
        }
        LOGGER.warn("creating default property definition because a previous definition could not be found for property \"" + name + "\" of type " + valueClass);
        propertyDefinition = new PropertyDefinition(name, valueClass, TextIndexHint.ALL);
        savePropertyDefinition(propertyDefinition);
    }

    protected Class<? extends Value> getValueType(Value value) {
        Class<? extends Value> valueClass = value.getClass();
        if (value instanceof StreamingPropertyValue) {
            valueClass = ((StreamingPropertyValue) value).getValueType();
        } else if (value instanceof StreamingPropertyValueRef) {
            valueClass = ((StreamingPropertyValueRef) value).getValueType();
        }
        return valueClass;
    }

    @Override
    public Iterable<Element> saveElementMutations(
            Iterable<ElementMutation<? extends Element>> mutations,
            Authorizations authorizations
    ) {
        List<Element> elements = new ArrayList<>();
        for (ElementMutation m : orderMutations(mutations)) {
            if (m instanceof ExistingElementMutation && !m.hasChanges()) {
                elements.add(((ExistingElementMutation) m).getElement());
                continue;
            }

            Element element = m.save(authorizations);
            elements.add(element);
        }
        return elements;
    }

    protected Iterable<ElementMutation> orderMutations(Iterable<ElementMutation<? extends Element>> mutations) {
        List<ElementMutation> orderedMutations = new ArrayList<>();
        orderedMutations.addAll(StreamUtils.stream(mutations)
                .filter(m -> m.getElementType().equals(ElementType.VERTEX))
                .collect(Collectors.toList())
        );
        orderedMutations.addAll(StreamUtils.stream(mutations)
                .filter(m -> m.getElementType().equals(ElementType.EDGE))
                .collect(Collectors.toList())
        );
        return orderedMutations;
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> idsIterable, FetchHints fetchHints, Authorizations authorizations) {
        Set<ExtendedDataRowId> ids = Sets.newHashSet(idsIterable);
        return new FilterIterable<ExtendedDataRow>(getAllExtendedData(fetchHints, authorizations)) {
            @Override
            protected boolean isIncluded(ExtendedDataRow row) {
                return ids.contains(row.getId());
            }
        };
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedDataForElements(
            Iterable<? extends ElementId> elementIdsArg,
            String tableName,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        List<ElementId> elementIds = Lists.newArrayList(elementIdsArg);
        for (ElementId elementId : elementIds) {
            if ((elementId.getElementType() == null && (elementId.getId() != null || tableName != null))
                    || (elementId.getElementType() != null && elementId.getId() == null && tableName != null)) {
                throw new GeException("Cannot create partial key with missing inner value");
            }
        }

        return new FilterIterable<ExtendedDataRow>(getAllExtendedData(fetchHints, authorizations)) {
            @Override
            protected boolean isIncluded(ExtendedDataRow row) {
                ExtendedDataRowId rowId = row.getId();
                if (tableName != null && !tableName.equals(rowId.getTableName())) {
                    return false;
                }
                return elementIds.stream().anyMatch(
                        elementId -> {
                            if (elementId.getElementType() != null && !elementId.getElementType().equals(rowId.getElementType())) {
                                return false;
                            }
                            if (elementId.getId() != null && !elementId.getId().equals(rowId.getElementId())) {
                                return false;
                            }
                            return true;
                        }
                );
            }
        };
    }

    @Override
    public Iterable<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, IdRange elementIdRange, Authorizations authorizations) {
        return new FilterIterable<ExtendedDataRow>(getAllExtendedData(FetchHints.ALL, authorizations)) {
            @Override
            protected boolean isIncluded(ExtendedDataRow row) {
                ExtendedDataRowId rowId = row.getId();
                return elementType.equals(rowId.getElementType())
                        && elementIdRange.isInRange(rowId.getElementId());
            }
        };
    }

    protected Iterable<ExtendedDataRow> getAllExtendedData(FetchHints fetchHints, Authorizations authorizations) {
        JoinIterable<Element> allElements = new JoinIterable<>(getVertices(fetchHints, authorizations), getEdges(fetchHints, authorizations));
        return new SelectManyIterable<Element, ExtendedDataRow>(allElements) {
            @Override
            protected Iterable<? extends ExtendedDataRow> getIterable(Element element) {
                return new SelectManyIterable<String, ExtendedDataRow>(element.getExtendedDataTableNames()) {
                    @Override
                    protected Iterable<? extends ExtendedDataRow> getIterable(String tableName) {
                        return element.getExtendedData(tableName);
                    }
                };
            }
        };
    }

    protected void deleteAllExtendedDataForElement(Element element, Authorizations authorizations) {
        if (!element.getFetchHints().isIncludeExtendedDataTableNames()) {
            throw new GeMissingFetchHintException(element.getFetchHints(), "includeExtendedDataTableNames");
        }
        if (element.getExtendedDataTableNames().size() == 0) {
            return;
        }
        FetchHints fetchHints = new FetchHintsBuilder()
                .setIncludeExtendedDataTableNames(true)
                .build();
        Iterable<ExtendedDataRow> rows = getExtendedData(
                element.getElementType(),
                element.getId(),
                null,
                fetchHints,
                authorizations
        );
        for (ExtendedDataRow row : rows) {
            deleteExtendedDataRow(row.getId(), authorizations);
        }
    }

    @Override
    public void dumpGraph() {
    }

    @Override
    public GeMetricRegistry getMetricsRegistry() {
        return metricRegistry;
    }
}
