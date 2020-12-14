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
import com.mware.ge.id.IdGenerator;
import com.mware.ge.id.IdentityNameSubstitutionStrategy;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.metric.StackTraceTracker;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValueRef;
import com.mware.ge.query.GraphQuery;
import com.mware.ge.query.MultiVertexQuery;
import com.mware.ge.query.SimilarToGraphQuery;
import com.mware.ge.util.*;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;

import java.io.InputStream;
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
    public Vertex addVertex(Visibility visibility, Authorizations authorizations, String conceptType) {
        return prepareVertex(visibility, conceptType).save(authorizations);
    }

    @Override
    public Vertex addVertex(String vertexId, Visibility visibility, Authorizations authorizations, String conceptType) {
        return prepareVertex(vertexId, visibility, conceptType).save(authorizations);
    }

    @Override
    public Iterable<Vertex> addVertices(Iterable<ElementBuilder<Vertex>> vertices, Authorizations authorizations) {
        List<Vertex> addedVertices = new ArrayList<>();
        for (ElementBuilder<Vertex> vertexBuilder : vertices) {
            addedVertices.add(vertexBuilder.save(authorizations));
        }
        return addedVertices;
    }

    @Override
    public VertexBuilder prepareVertex(Visibility visibility, String conceptType) {
        return prepareVertex(getIdGenerator().nextId(),  null, visibility, conceptType);
    }

    @Override
    public abstract VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility, String conceptType);

    @Override
    public VertexBuilder prepareVertex(Long timestamp, Visibility visibility, String conceptType) {
        return prepareVertex(getIdGenerator().nextId(), timestamp, visibility, conceptType);
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Visibility visibility, String conceptType) {
        return prepareVertex(vertexId,  null, visibility, conceptType);
    }

    @Override
    public boolean doesVertexExist(String vertexId, Authorizations authorizations) {
        return getVertex(vertexId, FetchHints.NONE, authorizations) != null;
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Authorizations authorizations) {
        return getVertex(vertexId, fetchHints, null, authorizations);
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        if (null == fetchHints) {
            return getVertex(vertexId, getDefaultFetchHints(), endTime, authorizations);
        }

        for (Vertex vertex : getVertices(fetchHints, endTime, authorizations)) {
            if (vertex.getId().equals(vertexId)) {
                return vertex;
            }
        }
        return null;
    }

    @Override
    public Vertex getVertex(String vertexId, Authorizations authorizations) throws GeException {
        return getVertex(vertexId, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, Authorizations authorizations) {
        return getVerticesWithPrefix(vertexIdPrefix, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Authorizations authorizations) {
        return getVerticesWithPrefix(vertexIdPrefix, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesWithPrefix(final String vertexIdPrefix, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<Vertex> vertices = getVertices(fetchHints, endTime, authorizations);
        return new FilterIterable<Vertex>(vertices) {
            @Override
            protected boolean isIncluded(Vertex v) {
                return v.getId().startsWith(vertexIdPrefix);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(IdRange idRange, Authorizations authorizations) {
        return getVerticesInRange(idRange, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(IdRange idRange, FetchHints fetchHints, Authorizations authorizations) {
        return getVerticesInRange(idRange, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Vertex> getVerticesInRange(final IdRange idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<Vertex> vertices = getVertices(fetchHints, endTime, authorizations);
        return new FilterIterable<Vertex>(vertices) {
            @Override
            protected boolean isIncluded(Vertex v) {
                return idRange.isInRange(v.getId());
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(final Iterable<String> ids, FetchHints fetchHints, final Authorizations authorizations) {
        return getVertices(ids, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(final Iterable<String> ids, final FetchHints fetchHints, final Long endTime, final Authorizations authorizations) {
        return new LookAheadIterable<String, Vertex>() {
            @Override
            protected boolean isIncluded(String src, Vertex vertex) {
                return vertex != null;
            }

            @Override
            protected Vertex convert(String id) {
                return getVertex(id, fetchHints, endTime, authorizations);
            }

            @Override
            protected Iterator<String> createIterator() {
                return Sets.newHashSet(ids).iterator();
            }
        };
    }

    @Override
    public Map<String, Boolean> doVerticesExist(Iterable<String> ids, Authorizations authorizations) {
        Map<String, Boolean> results = new HashMap<>();
        for (String id : ids) {
            results.put(id, false);
        }
        for (Vertex vertex : getVertices(ids, FetchHints.NONE, authorizations)) {
            results.put(vertex.getId(), true);
        }
        return results;
    }

    @Override
    public Iterable<Vertex> getVertices(final Iterable<String> ids, final Authorizations authorizations) {
        return getVertices(ids, getDefaultFetchHints(), authorizations);
    }

    @Override
    public List<Vertex> getVerticesInOrder(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations) {
        final List<String> vertexIds = IterableUtils.toList(ids);
        List<Vertex> vertices = IterableUtils.toList(getVertices(vertexIds, authorizations));
        vertices.sort((v1, v2) -> {
            Integer i1 = vertexIds.indexOf(v1.getId());
            Integer i2 = vertexIds.indexOf(v2.getId());
            return i1.compareTo(i2);
        });
        return vertices;
    }

    @Override
    public List<Vertex> getVerticesInOrder(Iterable<String> ids, Authorizations authorizations) {
        return getVerticesInOrder(ids, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Authorizations authorizations) throws GeException {
        return getVertices(getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(FetchHints fetchHints, Authorizations authorizations) {
        return getVertices(fetchHints, null, authorizations);
    }

    @Override
    public abstract Iterable<Vertex> getVertices(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    @Override
    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(outVertex, inVertex, label, visibility).save(authorizations);
    }

    @Override
    public Edge addEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertex, inVertex, label, visibility).save(authorizations);
    }

    @Override
    public Edge addEdge(String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(outVertexId, inVertexId, label, visibility).save(authorizations);
    }

    @Override
    public Edge addEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, visibility).save(authorizations);
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String outVertexId, String inVertexId, String label, Visibility visibility) {
        return prepareEdge(getIdGenerator().nextId(), outVertexId, inVertexId, label, visibility);
    }

    @Override
    public EdgeBuilder prepareEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(getIdGenerator().nextId(), outVertex, inVertex, label, visibility);
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertex, inVertex, label, null, visibility);
    }

    @Override
    public abstract EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility);

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, null, visibility);
    }

    @Override
    public abstract EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility);

    @Override
    public boolean doesEdgeExist(String edgeId, Authorizations authorizations) {
        return getEdge(edgeId, FetchHints.NONE, authorizations) != null;
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Authorizations authorizations) {
        return getEdge(edgeId, fetchHints, null, authorizations);
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        if (null == fetchHints) {
            return getEdge(edgeId, getDefaultFetchHints(), endTime, authorizations);
        }

        for (Edge edge : getEdges(fetchHints, endTime, authorizations)) {
            if (edge.getId().equals(edgeId)) {
                return edge;
            }
        }
        return null;
    }

    @Override
    public Edge getEdge(String edgeId, Authorizations authorizations) {
        return getEdge(edgeId, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Map<String, Boolean> doEdgesExist(Iterable<String> ids, Authorizations authorizations) {
        return doEdgesExist(ids, null, authorizations);
    }

    @Override
    public Map<String, Boolean> doEdgesExist(Iterable<String> ids, Long endTime, Authorizations authorizations) {
        Map<String, Boolean> results = new HashMap<>();
        for (String id : ids) {
            results.put(id, false);
        }
        for (Edge edge : getEdges(ids, FetchHints.NONE, endTime, authorizations)) {
            results.put(edge.getId(), true);
        }
        return results;
    }

    @Override
    public void deleteVertex(String vertexId, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to delete with id: " + vertexId);
        deleteVertex(vertex, authorizations);
    }

    @Override
    public void deleteEdge(String edgeId, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to delete with id: " + edgeId);
        deleteEdge(edge, authorizations);
    }

    @Override
    public void softDeleteVertex(String vertexId, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to soft delete with id: " + vertexId);
        softDeleteVertex(vertex, null, authorizations);
    }

    @Override
    public void softDeleteVertex(String vertexId, Long timestamp, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to soft delete with id: " + vertexId);
        softDeleteVertex(vertex, timestamp, authorizations);
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Authorizations authorizations) {
        softDeleteVertex(vertex, null, authorizations);
    }

    @Override
    public abstract void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations);

    @Override
    public void softDeleteEdge(String edgeId, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to soft delete with id: " + edgeId);
        softDeleteEdge(edge, null, authorizations);
    }

    @Override
    public void softDeleteEdge(String edgeId, Long timestamp, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to soft delete with id: " + edgeId);
        softDeleteEdge(edge, timestamp, authorizations);
    }

    @Override
    public void softDeleteEdge(Edge edge, Authorizations authorizations) {
        softDeleteEdge(edge, null, authorizations);
    }

    @Override
    public abstract void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations);

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
    public Iterable<Edge> getEdges(final Iterable<String> ids, final FetchHints fetchHints, final Authorizations authorizations) {
        return getEdges(ids, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Iterable<String> ids, final FetchHints fetchHints, final Long endTime, final Authorizations authorizations) {
        return new LookAheadIterable<String, Edge>() {
            @Override
            protected boolean isIncluded(String src, Edge edge) {
                return edge != null;
            }

            @Override
            protected Edge convert(String id) {
                return getEdge(id, fetchHints, endTime, authorizations);
            }

            @Override
            protected Iterator<String> createIterator() {
                return Sets.newHashSet(ids).iterator();
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final Iterable<String> ids, final Authorizations authorizations) {
        return getEdges(ids, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Authorizations authorizations) {
        return getEdges(getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(FetchHints fetchHints, Authorizations authorizations) {
        return getEdges(fetchHints, null, authorizations);
    }

    @Override
    public abstract Iterable<Edge> getEdges(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    @Override
    public Iterable<Edge> getEdgesInRange(IdRange idRange, Authorizations authorizations) {
        return getEdgesInRange(idRange, getDefaultFetchHints(), authorizations);
    }

    @Override
    public Iterable<Edge> getEdgesInRange(IdRange idRange, FetchHints fetchHints, Authorizations authorizations) {
        return getEdgesInRange(idRange, fetchHints, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdgesInRange(final IdRange idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<Edge> edges = getEdges(fetchHints, endTime, authorizations);
        return new FilterIterable<Edge>(edges) {
            @Override
            protected boolean isIncluded(Edge e) {
                return idRange.isInRange(e.getId());
            }
        };
    }

    @Override
    @Deprecated
    public Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, Authorizations authorizations) {
        return findPaths(sourceVertexId, destVertexId, null, maxHops, authorizations);
    }

    @Override
    @Deprecated
    public Iterable<Path> findPaths(String sourceVertexId, String destVertexId, String[] labels, int maxHops, Authorizations authorizations) {
        return findPaths(sourceVertexId, destVertexId, labels, maxHops, null, authorizations);
    }

    @Override
    @Deprecated
    public Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, ProgressCallback progressCallback, Authorizations authorizations) {
        return findPaths(sourceVertexId, destVertexId, null, maxHops, progressCallback, authorizations);
    }

    @Override
    @Deprecated
    public Iterable<Path> findPaths(String sourceVertexId, String destVertexId, String[] labels, int maxHops, ProgressCallback progressCallback, Authorizations authorizations) {
        FindPathOptions options = new FindPathOptions(sourceVertexId, destVertexId, maxHops);
        options.setLabels(labels);
        options.setProgressCallback(progressCallback);
        return findPaths(options, authorizations);
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

    @Override
    @Deprecated
    public Iterable<String> findRelatedEdges(Iterable<String> vertexIds, Authorizations authorizations) {
        return findRelatedEdgeIds(vertexIds, authorizations);
    }

    @Override
    @Deprecated
    public Iterable<String> findRelatedEdges(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        return findRelatedEdgeIds(vertexIds, endTime, authorizations);
    }

    @Override
    public Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Authorizations authorizations) {
        return findRelatedEdgeIds(vertexIds, null, authorizations);
    }

    @Override
    public Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        FetchHints fetchHints = new FetchHintsBuilder()
                .setIncludeOutEdgeRefs(true)
                .build();
        return findRelatedEdgeIdsForVertices(getVertices(vertexIds, fetchHints, endTime, authorizations), authorizations);
    }

    @Override
    public Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Authorizations authorizations) {
        return findRelatedEdgeSummary(vertexIds, null, authorizations);
    }

    @Override
    public Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        FetchHints fetchHints = new FetchHintsBuilder()
                .setIncludeOutEdgeRefs(true)
                .build();
        return findRelatedEdgeSummaryForVertices(getVertices(vertexIds, fetchHints, endTime, authorizations), authorizations);
    }

    @Override
    public Iterable<RelatedEdge> findRelatedEdgeSummaryForVertices(Iterable<Vertex> verticesIterable, Authorizations authorizations) {
        List<RelatedEdge> results = new ArrayList<>();
        List<Vertex> vertices = IterableUtils.toList(verticesIterable);
        for (Vertex outVertex : vertices) {
            Iterable<EdgeInfo> edgeInfos = outVertex.getEdgeInfos(Direction.OUT, authorizations);
            for (EdgeInfo edgeInfo : edgeInfos) {
                for (Vertex inVertex : vertices) {
                    if (edgeInfo.getVertexId().equals(inVertex.getId())) {
                        results.add(new RelatedEdgeImpl(edgeInfo.getEdgeId(), edgeInfo.getLabel(), outVertex.getId(), inVertex.getId()));
                    }
                }
            }
        }
        return results;
    }

    @Override
    public Iterable<String> findRelatedEdgeIdsForVertices(Iterable<Vertex> verticesIterable, Authorizations authorizations) {
        List<String> results = new ArrayList<>();
        List<Vertex> vertices = IterableUtils.toList(verticesIterable);
        for (Vertex outVertex : vertices) {
            if (outVertex == null) {
                throw new GeException("verticesIterable cannot have null values");
            }
            Iterable<EdgeInfo> edgeInfos = outVertex.getEdgeInfos(Direction.OUT, authorizations);
            for (EdgeInfo edgeInfo : edgeInfos) {
                for (Vertex inVertex : vertices) {
                    if (edgeInfo.getVertexId() == null) { // This check is for legacy data. null EdgeInfo.vertexIds are no longer permitted
                        continue;
                    }
                    if (edgeInfo.getVertexId().equals(inVertex.getId())) {
                        results.add(edgeInfo.getEdgeId());
                    }
                }
            }
        }
        return results;
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
    public abstract GraphQuery query(Authorizations authorizations);

    @Override
    public abstract GraphQuery query(String queryString, Authorizations authorizations);

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
    public boolean isQuerySimilarToTextSupported() {
        return false;
    }

    @Override
    public SimilarToGraphQuery querySimilarTo(String[] fields, String text, Authorizations authorizations) {
        throw new GeNotSupportedException("querySimilarTo not supported");
    }

    @Override
    public Authorizations createAuthorizations(Collection<String> auths) {
        checkNotNull(auths, "auths cannot be null");
        return createAuthorizations(auths.toArray(new String[auths.size()]));
    }

    @Override
    public Authorizations createAuthorizations(Authorizations auths, String... additionalAuthorizations) {
        Set<String> newAuths = new HashSet<>();
        Collections.addAll(newAuths, auths.getAuthorizations());
        Collections.addAll(newAuths, additionalAuthorizations);
        return createAuthorizations(newAuths);
    }

    @Override
    public Authorizations createAuthorizations(Authorizations auths, Collection<String> additionalAuthorizations) {
        return createAuthorizations(auths, additionalAuthorizations.toArray(new String[additionalAuthorizations.size()]));
    }

    @Override
    public Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations) {
        Map<Object, Long> countsByValue = new HashMap<>();
        for (Vertex v : getVertices(authorizations)) {
            for (Property p : v.getProperties()) {
                if (propertyName.equals(p.getName())) {
                    Value mapKey = p.getValue();
                    if (mapKey instanceof TextValue) {
                        mapKey = ((TextValue) mapKey).toLower();
                    }
                    Long currentValue = countsByValue.get(mapKey.asObjectCopy());
                    if (currentValue == null) {
                        countsByValue.put(mapKey.asObjectCopy(), 1L);
                    } else {
                        countsByValue.put(mapKey.asObjectCopy(), currentValue + 1);
                    }
                }
            }
        }
        return countsByValue;
    }

    @Override
    public long getVertexCount(Authorizations authorizations) {
        return count(getVertices(authorizations));
    }

    @Override
    public long getEdgeCount(Authorizations authorizations) {
        return count(getEdges(authorizations));
    }

    @Override
    public abstract void deleteVertex(Vertex vertex, Authorizations authorizations);

    @Override
    public abstract void deleteEdge(Edge edge, Authorizations authorizations);

    @Override
    public abstract void deleteExtendedDataRow(ExtendedDataRowId id, Authorizations authorizations);

    @Override
    public abstract MultiVertexQuery query(String[] vertexIds, String queryString, Authorizations authorizations);

    @Override
    public abstract MultiVertexQuery query(String[] vertexIds, Authorizations authorizations);

    @Override
    public abstract IdGenerator getIdGenerator();

    @Override
    public abstract boolean isVisibilityValid(Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void truncate();

    @Override
    public abstract void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations);

    @Override
    public abstract Authorizations createAuthorizations(String... auths);

    @Override
    public DefinePropertyBuilder defineProperty(String propertyName) {
        return new DefinePropertyBuilder(propertyName) {
            @Override
            public PropertyDefinition define() {
                PropertyDefinition propertyDefinition = super.define();
                savePropertyDefinition(propertyDefinition);
                return propertyDefinition;
            }
        };
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
    public List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        return streamingPropertyValues.stream()
                .map(StreamingPropertyValue::getInputStream)
                .collect(Collectors.toList());
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
    public ExtendedDataRow getExtendedData(ExtendedDataRowId id, Authorizations authorizations) {
        ArrayList<ExtendedDataRow> rows = Lists.newArrayList(getExtendedData(Lists.newArrayList(id), authorizations));
        if (rows.size() == 0) {
            return null;
        }
        if (rows.size() == 1) {
            return rows.get(0);
        }
        throw new GeException("Expected 0 or 1 rows found " + rows.size());
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
    public void visitElements(GraphVisitor graphVisitor, Authorizations authorizations) {
        visitVertices(graphVisitor, authorizations);
        visitEdges(graphVisitor, authorizations);
    }

    @Override
    public void visitVertices(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getVertices(authorizations), graphVisitor);
    }

    @Override
    public void visitEdges(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getEdges(authorizations), graphVisitor);
    }

    @Override
    public void visit(Iterable<? extends Element> elements, GraphVisitor visitor) {
        int i = 0;
        for (Element element : elements) {
            if (i % 1000000 == 0) {
                LOGGER.debug("checking: %s", element.getId());
            }

            visitor.visitElement(element);
            if (element instanceof Vertex) {
                visitor.visitVertex((Vertex) element);
            } else if (element instanceof Edge) {
                visitor.visitEdge((Edge) element);
            } else {
                throw new GeException("Invalid element type to visit: " + element.getClass().getName());
            }

            for (Property property : element.getProperties()) {
                visitor.visitProperty(element, property);
            }

            for (String tableName : element.getExtendedDataTableNames()) {
                for (ExtendedDataRow extendedDataRow : element.getExtendedData(tableName)) {
                    visitor.visitExtendedDataRow(element, tableName, extendedDataRow);
                    for (Property property : extendedDataRow.getProperties()) {
                        visitor.visitProperty(element, tableName, extendedDataRow, property);
                    }
                }
            }

            i++;
        }
    }

    @Override
    public Element getElement(ElementId elementId, Authorizations authorizations) {
        return getElement(elementId, getDefaultFetchHints(), authorizations);
    }

    @Override
    public void dumpGraph() {
    }

    @Override
    public GeMetricRegistry getMetricsRegistry() {
        return metricRegistry;
    }

    @Override
    public NameSubstitutionStrategy getNameSubstitutionStrategy() {
        return new IdentityNameSubstitutionStrategy();
    }
}
