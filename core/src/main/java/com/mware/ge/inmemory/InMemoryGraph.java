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
package com.mware.ge.inmemory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mware.ge.*;
import com.mware.ge.event.*;
import com.mware.ge.id.IdGenerator;
import com.mware.ge.id.UUIDIdGenerator;
import com.mware.ge.inmemory.mutations.*;
import com.mware.ge.mutation.*;
import com.mware.ge.property.PropertyDescriptor;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValueRef;
import com.mware.ge.search.IndexHint;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.util.*;
import com.mware.ge.values.storable.Value;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Stream;

import static com.mware.ge.util.Preconditions.checkNotNull;
import static com.mware.ge.util.StreamUtils.stream;

public class InMemoryGraph extends GraphBaseWithSearchIndex {
    protected static final InMemoryGraphConfiguration DEFAULT_CONFIGURATION = new InMemoryGraphConfiguration(new HashMap<>());
    private final Set<String> validAuthorizations = new HashSet<>();
    private final InMemoryVertexTable verticesTable;
    private final InMemoryEdgeTable edgesTable;
    private final InMemoryExtendedDataTable extendedDataTable;
    private final GraphMetadataStore graphMetadataStore;

    protected InMemoryGraph(InMemoryGraphConfiguration configuration) {
        this(
                configuration,
                new InMemoryVertexTable(),
                new InMemoryEdgeTable(),
                new MapInMemoryExtendedDataTable()
        );
    }

    protected InMemoryGraph(InMemoryGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this(
                configuration,
                idGenerator,
                searchIndex,
                new InMemoryVertexTable(),
                new InMemoryEdgeTable(),
                new MapInMemoryExtendedDataTable()
        );
    }

    protected InMemoryGraph(
            InMemoryGraphConfiguration configuration,
            InMemoryVertexTable verticesTable,
            InMemoryEdgeTable edgesTable,
            InMemoryExtendedDataTable extendedDataTable
    ) {
        super(configuration);
        this.verticesTable = verticesTable;
        this.edgesTable = edgesTable;
        this.extendedDataTable = extendedDataTable;
        this.graphMetadataStore = newGraphMetadataStore(configuration);
    }

    protected InMemoryGraph(
            InMemoryGraphConfiguration configuration,
            IdGenerator idGenerator,
            SearchIndex searchIndex,
            InMemoryVertexTable verticesTable,
            InMemoryEdgeTable edgesTable,
            InMemoryExtendedDataTable extendedDataTable
    ) {
        super(configuration, idGenerator, searchIndex);
        this.verticesTable = verticesTable;
        this.edgesTable = edgesTable;
        this.extendedDataTable = extendedDataTable;
        this.graphMetadataStore = newGraphMetadataStore(configuration);
    }

    protected GraphMetadataStore newGraphMetadataStore(GraphConfiguration configuration) {
        return new InMemoryGraphMetadataStore();
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create() {
        DEFAULT_CONFIGURATION.set(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        return create(DEFAULT_CONFIGURATION);
    }

    public static InMemoryGraph create(InMemoryGraphConfiguration config) {
        InMemoryGraph graph = new InMemoryGraph(config);
        graph.setup();
        return graph;
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create(Map<String, Object> config) {
        return create(new InMemoryGraphConfiguration(config));
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create(InMemoryGraphConfiguration config, IdGenerator idGenerator, SearchIndex searchIndex) {
        InMemoryGraph graph = new InMemoryGraph(config, idGenerator, searchIndex);
        graph.setup();
        return graph;
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility, String conceptType) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        return new VertexBuilder(vertexId, conceptType, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                addValidAuthorizations(authorizations.getAuthorizations());
                boolean isNew = false;
                InMemoryTableElement vertexTableElement = InMemoryGraph.this.verticesTable.getTableElement(getId());
                if (vertexTableElement == null) {
                    isNew = true;
                    verticesTable.append(
                            getId(),
                            new AlterVisibilityMutation(timestampLong, getVisibility()),
                            new ElementTimestampMutation(timestampLong),
                            new AlterConceptTypeMutation(timestampLong, getConceptType())
                    );
                } else {
                    if (vertexTableElement.getVisibility().equals(getVisibility())) {
                        verticesTable.append(getId(), new ElementTimestampMutation(timestampLong));
                    } else {
                        verticesTable.append(getId(), new AlterVisibilityMutation(timestampLong, getVisibility()), new ElementTimestampMutation(timestampLong));
                    }
                }

                if (getNewConceptType() != null) {
                    verticesTable.append(getId(), new AlterConceptTypeMutation(timestampLong, getNewConceptType()));
                }

                InMemoryVertex vertex = InMemoryGraph.this.verticesTable.get(InMemoryGraph.this, getId(), FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
                if (isNew && hasEventListeners()) {
                    fireGraphEvent(new AddVertexEvent(InMemoryGraph.this, vertex));
                }

                vertex.updatePropertiesInternal(this);

                // to more closely simulate how accumulo works. add a potentially sparse (in case of an update) vertex to the search index.
                if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                    updateElementAndExtendedDataInSearchIndex(vertex, this, authorizations);
                }

                return vertex;
            }
        };
    }

    <T extends Element> void updateElementAndExtendedDataInSearchIndex(
            Element element,
            ElementMutation<T> elementMutation,
            Authorizations authorizations
    ) {
        if (elementMutation instanceof ExistingElementMutation) {
            getSearchIndex().updateElement(this, (ExistingElementMutation<? extends Element>) elementMutation, authorizations);
        } else {
            getSearchIndex().addElement(
                    this,
                    element,
                    authorizations
            );
        }
        getSearchIndex().addElementExtendedData(
                InMemoryGraph.this,
                element,
                elementMutation.getExtendedData(),
                authorizations
        );
        for (ExtendedDataDeleteMutation m : elementMutation.getExtendedDataDeletes()) {
            if (StringUtils.isEmpty(m.getColumnName())) {
                deleteExtendedDataRow(new ExtendedDataRowId(element.getElementType(), element.getId(), m.getTableName(), m.getRow()), authorizations);
            } else {
                deleteExtendedData(
                        (InMemoryElement) element,
                        m.getTableName(),
                        m.getRow(),
                        m.getColumnName(),
                        m.getKey(),
                        m.getVisibility(),
                        authorizations);
            }
        }
    }

    private void addValidAuthorizations(String[] authorizations) {
        Collections.addAll(this.validAuthorizations, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(FetchHints fetchHints, final Long endTime, final Authorizations authorizations) throws GeException {
        validateAuthorizations(authorizations);
        return new ConvertingIterable<InMemoryVertex, Vertex>(this.verticesTable.getAll(InMemoryGraph.this, fetchHints, endTime, authorizations)) {
            @Override
            protected Vertex convert(InMemoryVertex o) {
                return o;
            }
        };
    }

    @Override
    public Iterable<String> getVertexIds(Authorizations authorizations) {
        validateAuthorizations(authorizations);
        return new ConvertingIterable<InMemoryVertex, String>(this.verticesTable.getAll(InMemoryGraph.this, FetchHints.NONE, null, authorizations)) {
            @Override
            protected String convert(InMemoryVertex o) {
                return o.getId();
            }
        };
    }


    protected void validateAuthorizations(Authorizations authorizations) {
        for (String auth : authorizations.getAuthorizations()) {
            if (!this.validAuthorizations.contains(auth)) {
                throw new SecurityGeException("Invalid authorizations", authorizations);
            }
        }
    }

    @Override
    public void deleteVertex(Vertex vertex, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }

        List<Edge> edgesToDelete = IterableUtils.toList(vertex.getEdges(Direction.BOTH, authorizations));
        for (Edge edgeToDelete : edgesToDelete) {
            deleteEdge(edgeToDelete, authorizations);
        }

        deleteAllExtendedDataForElement(vertex, authorizations);

        this.verticesTable.remove(vertex.getId());
        getSearchIndex().deleteElement(this, vertex, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeleteVertexEvent(this, vertex));
        }
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        for (Property property : vertex.getProperties()) {
            vertex.softDeleteProperty(property.getKey(), property.getName(), property.getVisibility(), authorizations);
        }

        List<Edge> edgesToSoftDelete = IterableUtils.toList(vertex.getEdges(Direction.BOTH, authorizations));
        for (Edge edgeToSoftDelete : edgesToSoftDelete) {
            softDeleteEdge(edgeToSoftDelete, timestamp, authorizations);
        }

        this.verticesTable.getTableElement(vertex.getId()).appendSoftDeleteMutation(timestamp);

        getSearchIndex().deleteElement(this, vertex, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new SoftDeleteVertexEvent(this, vertex));
        }
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }

        List<Edge> edgesToMarkHidden = IterableUtils.toList(vertex.getEdges(Direction.BOTH, authorizations));
        for (Edge edgeToMarkHidden : edgesToMarkHidden) {
            markEdgeHidden(edgeToMarkHidden, visibility, authorizations);
        }

        this.verticesTable.getTableElement(vertex.getId()).appendMarkHiddenMutation(visibility);
        refreshVertexInMemoryTableElement(vertex);
        getSearchIndex().markElementHidden(this, vertex, visibility, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkHiddenVertexEvent(this, vertex));
        }
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations) {
        if (!((InMemoryVertex) vertex).canRead(authorizations)) {
            return;
        }

        List<Edge> edgesToMarkVisible = IterableUtils.toList(vertex.getEdges(Direction.BOTH, FetchHints.ALL_INCLUDING_HIDDEN, authorizations));
        for (Edge edgeToMarkVisible : edgesToMarkVisible) {
            markEdgeVisible(edgeToMarkVisible, visibility, authorizations);
        }

        this.verticesTable.getTableElement(vertex.getId()).appendMarkVisibleMutation(visibility);
        refreshVertexInMemoryTableElement(vertex);
        getSearchIndex().markElementVisible(this, vertex, visibility, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkVisibleVertexEvent(this, vertex));
        }
    }

    public void markPropertyHidden(
            InMemoryElement element,
            InMemoryTableElement inMemoryTableElement,
            Property property,
            Long timestamp,
            Visibility visibility,
            Authorizations authorizations
    ) {
        if (!element.canRead(authorizations)) {
            return;
        }

        Property hiddenProperty = inMemoryTableElement.appendMarkPropertyHiddenMutation(
                property.getKey(),
                property.getName(),
                property.getVisibility(),
                timestamp,
                visibility,
                authorizations
        );

        getSearchIndex().markPropertyHidden(this, element, property, visibility, authorizations);

        if (hiddenProperty != null && hasEventListeners()) {
            fireGraphEvent(new MarkHiddenPropertyEvent(this, element, hiddenProperty, visibility));
        }
    }

    public void markPropertyVisible(InMemoryElement element, InMemoryTableElement inMemoryTableElement, String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        if (!element.canRead(authorizations)) {
            return;
        }

        Property property = inMemoryTableElement.appendMarkPropertyVisibleMutation(key, name, propertyVisibility, timestamp, visibility, authorizations);

        getSearchIndex().markPropertyVisible(this, element, property, visibility, authorizations);

        if (property != null && hasEventListeners()) {
            fireGraphEvent(new MarkVisiblePropertyEvent(this, element, property, visibility));
        }
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, final Long timestamp, Visibility visibility) {
        checkNotNull(outVertexId, "outVertexId cannot be null");
        checkNotNull(inVertexId, "inVertexId cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilderByVertexId(edgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                addValidAuthorizations(authorizations.getAuthorizations());
                return savePreparedEdge(this, getVertexId(Direction.OUT), getVertexId(Direction.IN), timestamp, authorizations);
            }
        };
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, final Long timestamp, Visibility visibility) {
        checkNotNull(outVertex, "outVertex cannot be null");
        checkNotNull(inVertex, "inVertex cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                addValidAuthorizations(authorizations.getAuthorizations());
                return savePreparedEdge(this, getOutVertex().getId(), getInVertex().getId(), timestamp, authorizations);
            }
        };
    }

    private Edge savePreparedEdge(
            EdgeBuilderBase edgeBuilder,
            String outVertexId,
            String inVertexId,
            Long timestamp,
            Authorizations authorizations
    ) {
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();

            // The timestamps will be incremented below, this will ensure future mutations will be in the future
            IncreasingTime.advanceTime(10);
        }
        long incrementingTimestamp = timestamp;
        InMemoryTableEdge edgeTableElement = (InMemoryTableEdge)  this.edgesTable.getTableElement(edgeBuilder.getId());
        boolean isNew = false;
        if (edgeTableElement == null) {
            isNew = true;
            edgesTable.append(
                    edgeBuilder.getId(),
                    new AlterVisibilityMutation(incrementingTimestamp++, edgeBuilder.getVisibility()),
                    new ElementTimestampMutation(incrementingTimestamp++),
                    new AlterEdgeLabelMutation(incrementingTimestamp++, edgeBuilder.getEdgeLabel()),
                    new EdgeSetupMutation(incrementingTimestamp++, outVertexId, inVertexId)
            );
        } else {
            if (!outVertexId.equals(edgeTableElement.getOutVertexId()) || !inVertexId.equals(edgeTableElement.getInVertexId())) {
                edgesTable.append(
                        edgeBuilder.getId(),
                        new EdgeSetupMutation(incrementingTimestamp++, outVertexId, inVertexId)
                );
            }

            edgesTable.append(edgeBuilder.getId(), new ElementTimestampMutation(incrementingTimestamp++));
        }
        if (edgeBuilder.getNewEdgeLabel() != null) {
            edgesTable.append(edgeBuilder.getId(), new AlterEdgeLabelMutation(incrementingTimestamp, edgeBuilder.getNewEdgeLabel()));
        }

        InMemoryEdge edge = this.edgesTable.get(InMemoryGraph.this, edgeBuilder.getId(), FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        if (isNew && hasEventListeners()) {
            fireGraphEvent(new AddEdgeEvent(InMemoryGraph.this, edge));
        }
        edge.updatePropertiesInternal(edgeBuilder);

        if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            updateElementAndExtendedDataInSearchIndex(edge, edgeBuilder, authorizations);
        }

        return edge;
    }

    @Override
    public Iterable<Edge> getEdges(FetchHints fetchHints, final Long endTime, final Authorizations authorizations) {
        return new ConvertingIterable<InMemoryEdge, Edge>(this.edgesTable.getAll(InMemoryGraph.this, fetchHints, endTime, authorizations)) {
            @Override
            protected Edge convert(InMemoryEdge o) {
                return o;
            }
        };
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        return graphMetadataStore;
    }

    @Override
    public void deleteEdge(Edge edge, Authorizations authorizations) {
        checkNotNull(edge, "Edge cannot be null");

        deleteAllExtendedDataForElement(edge, authorizations);

        this.edgesTable.remove(edge.getId());
        getSearchIndex().deleteElement(this, edge, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeleteEdgeEvent(this, edge));
        }
    }

    @Override
    public void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations) {
        checkNotNull(edge, "Edge cannot be null");
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        this.edgesTable.getTableElement(edge.getId()).appendSoftDeleteMutation(timestamp);

        getSearchIndex().deleteElement(this, edge, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new SoftDeleteEdgeEvent(this, edge));
        }
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations) {
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }

        Vertex inVertex = getVertex(edge.getVertexId(Direction.IN), authorizations);
        checkNotNull(inVertex, "Could not find in vertex \"" + edge.getVertexId(Direction.IN) + "\" on edge \"" + edge.getId() + "\"");
        Vertex outVertex = getVertex(edge.getVertexId(Direction.OUT), authorizations);
        checkNotNull(outVertex, "Could not find out vertex \"" + edge.getVertexId(Direction.OUT) + "\" on edge \"" + edge.getId() + "\"");

        this.edgesTable.getTableElement(edge.getId()).appendMarkHiddenMutation(visibility);
        getSearchIndex().markElementHidden(this, edge, visibility, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkHiddenEdgeEvent(this, edge));
        }
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations) {
        if (!((InMemoryEdge) edge).canRead(authorizations)) {
            return;
        }

        Vertex inVertex = getVertex(edge.getVertexId(Direction.IN), FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        checkNotNull(inVertex, "Could not find in vertex \"" + edge.getVertexId(Direction.IN) + "\" on edge \"" + edge.getId() + "\"");
        Vertex outVertex = getVertex(edge.getVertexId(Direction.OUT), FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        checkNotNull(outVertex, "Could not find out vertex \"" + edge.getVertexId(Direction.OUT) + "\" on edge \"" + edge.getId() + "\"");

        this.edgesTable.getTableElement(edge.getId()).appendMarkVisibleMutation(visibility);
        getSearchIndex().markElementVisible(this, edge, visibility, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new MarkVisibleEdgeEvent(this, edge));
        }
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        addValidAuthorizations(auths);
        return new Authorizations(auths);
    }

    @Override
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
        findPathsRecursive(
                options,
                foundPaths,
                sourceVertex.getId(),
                destVertex.getId(),
                hops,
                seenVertices,
                currentPath,
                progressCallback,
                authorizations
        );
    }

    protected void findPathsRecursive(
            FindPathOptions options,
            List<Path> foundPaths,
            String sourceVertexId,
            String destVertexId,
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

        seenVertices.add(sourceVertexId);
        if (sourceVertexId.equals(destVertexId)) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            Stream<Edge> edges = stream(getEdgesFromVertex(sourceVertexId, getDefaultFetchHints(), null, authorizations))
                    .filter(edge -> {
                        if (options.getExcludedLabels() != null) {
                            if (ArrayUtils.contains(options.getExcludedLabels(), edge.getLabel())) {
                                return false;
                            }
                        }
                        return options.getLabels() == null || ArrayUtils.contains(options.getLabels(), edge.getLabel());
                    });
            List<String> vertexIds = new ArrayList<>();
            edges.forEach(edge -> {
                if (edge.getOtherVertex(sourceVertexId, FetchHints.NONE, authorizations) != null) {
                    vertexIds.add(edge.getOtherVertexId(sourceVertexId));
                }
            });

            int vertexCount = 0;
            if (firstLevelRecursion) {
                vertexCount = vertexIds.size();
            }
            int i = 0;
            for (String childId : vertexIds) {
                if (firstLevelRecursion) {
                    // this will never get to 100% since i starts at 0. which is good. 100% signifies done and we still
                    // have work to do.
                    double progress = (double) i / (double) vertexCount;
                    progressCallback.progress(progress, ProgressCallback.Step.SEARCHING_EDGES, i + 1, vertexCount);
                }
                if (!seenVertices.contains(childId)) {
                    findPathsRecursive(
                            options,
                            foundPaths,
                            childId,
                            destVertexId,
                            hops - 1,
                            seenVertices,
                            new Path(currentPath, childId),
                            progressCallback,
                            authorizations
                    );
                }
                i++;
            }
        }
        seenVertices.remove(sourceVertexId);
    }

    protected Iterable<Edge> getEdgesFromVertex(
            String vertexId,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        return new LookAheadIterable<InMemoryTableEdge, Edge>() {
            @Override
            protected boolean isIncluded(InMemoryTableEdge inMemoryTableElement, Edge edge) {
                if (edge == null) {
                    return false; // edge deleted or outside of time range
                }

                EdgeSetupMutation edgeSetupMutation = inMemoryTableElement.findLastMutation(EdgeSetupMutation.class);
                String inVertexId = edgeSetupMutation.getInVertexId();
                checkNotNull(inVertexId, "inVertexId was null");
                String outVertexId = edgeSetupMutation.getOutVertexId();
                checkNotNull(outVertexId, "outVertexId was null");

                return (inVertexId.equals(vertexId) || outVertexId.equals(vertexId)) &&
                        InMemoryGraph.this.isIncluded(inMemoryTableElement, fetchHints, authorizations);
            }

            @Override
            protected Edge convert(InMemoryTableEdge inMemoryTableElement) {
                return inMemoryTableElement.createElement(InMemoryGraph.this, fetchHints, endTime, authorizations);
            }

            @Override
            protected Iterator<InMemoryTableEdge> createIterator() {
                return edgesTable.getAllTableElements().iterator();
            }
        };
    }

    protected boolean isIncluded(
            InMemoryTableElement element, FetchHints fetchHints,
            Authorizations authorizations
    ) {
        boolean includeHidden = fetchHints.isIncludeHidden();

        if (!element.canRead(fetchHints, authorizations)) {
            return false;
        }

        if (!includeHidden) {
            if (element.isHidden(authorizations)) {
                return false;
            }
        }

        return true;
    }

    protected boolean isIncludedInTimeSpan(
            InMemoryTableElement element, FetchHints fetchHints, Long endTime,
            Authorizations authorizations
    ) {
        boolean includeHidden = fetchHints.isIncludeHidden();

        if (!element.canRead(fetchHints, authorizations)) {
            return false;
        }
        if (!includeHidden && element.isHidden(authorizations)) {
            return false;
        }

        if (element.isDeleted(endTime, authorizations)) {
            return false;
        }

        if (endTime != null && element.getFirstTimestamp() > endTime) {
            return false;
        }

        return true;
    }

    protected void softDeleteProperty(InMemoryTableElement inMemoryTableElement, Property property, Long timestamp, IndexHint indexHint, Authorizations authorizations) {
        Element element;
        if (inMemoryTableElement instanceof InMemoryTableVertex) {
            inMemoryTableElement.appendSoftDeletePropertyMutation(property.getKey(), property.getName(), property.getVisibility(), timestamp);
            element = getVertex(inMemoryTableElement.getId(), FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        } else if (inMemoryTableElement instanceof InMemoryTableEdge) {
            inMemoryTableElement.appendSoftDeletePropertyMutation(property.getKey(), property.getName(), property.getVisibility(), timestamp);
            element = getEdge(inMemoryTableElement.getId(), FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        } else {
            throw new IllegalArgumentException("Unexpected element type: " + inMemoryTableElement.getClass().getName());
        }
        if (indexHint != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().deleteProperty(this, element, PropertyDescriptor.fromProperty(property), authorizations);
        }

        if (hasEventListeners()) {
            fireGraphEvent(new SoftDeletePropertyEvent(this, element, property));
        }
    }

    public void addPropertyValue(
            InMemoryElement element,
            InMemoryTableElement inMemoryTableElement,
            String key,
            String name,
            Value value,
            Metadata metadata,
            Visibility visibility,
            Long timestamp,
            Authorizations authorizations
    ) {
        ensurePropertyDefined(name, value);

        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }

        if (value instanceof StreamingPropertyValue) {
            value = saveStreamingPropertyValue(
                    element.getId(),
                    key,
                    name,
                    visibility,
                    timestamp,
                    (StreamingPropertyValue) value
            );
        }
        inMemoryTableElement.appendAddPropertyValueMutation(key, name, value, metadata, visibility, timestamp);
        Property property = inMemoryTableElement.getProperty(key, name, visibility, FetchHints.ALL_INCLUDING_HIDDEN, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new AddPropertyEvent(this, element, property));
        }
    }

    protected void alterElementVisibility(InMemoryTableElement inMemoryTableElement, Visibility newEdgeVisibility) {
        inMemoryTableElement.appendAlterVisibilityMutation(newEdgeVisibility);
    }

    protected void alterElementPropertyVisibilities(
            InMemoryTableElement inMemoryTableElement,
            List<AlterPropertyVisibility> alterPropertyVisibilities,
            Authorizations authorizations
    ) {
        for (AlterPropertyVisibility apv : alterPropertyVisibilities) {
            Property property = inMemoryTableElement.getProperty(
                    apv.getKey(),
                    apv.getName(),
                    apv.getExistingVisibility(),
                    FetchHints.ALL_INCLUDING_HIDDEN,
                    authorizations
            );
            if (property == null) {
                throw new GeException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            if (apv.getExistingVisibility() == null) {
                apv.setExistingVisibility(property.getVisibility());
            }
            Value value = property.getValue();
            Metadata metadata = property.getMetadata();

            inMemoryTableElement.appendSoftDeletePropertyMutation(
                    apv.getKey(),
                    apv.getName(),
                    apv.getExistingVisibility(),
                    apv.getTimestamp()
            );

            long newTimestamp = apv.getTimestamp() + 1;
            if (value instanceof StreamingPropertyValue) {
                value = saveStreamingPropertyValue(
                        inMemoryTableElement.getId(),
                        apv.getKey(),
                        apv.getName(),
                        apv.getVisibility(),
                        newTimestamp,
                        (StreamingPropertyValue) value
                );
            }
            inMemoryTableElement.appendAddPropertyValueMutation(
                    apv.getKey(),
                    apv.getName(),
                    value,
                    metadata,
                    apv.getVisibility(),
                    newTimestamp
            );
        }
    }

    protected void alterElementPropertyMetadata(
            InMemoryTableElement inMemoryTableElement, List<SetPropertyMetadata> setPropertyMetadatas,
            Authorizations authorizations
    ) {
        for (SetPropertyMetadata spm : setPropertyMetadatas) {
            Property property = inMemoryTableElement.getProperty(
                    spm.getPropertyKey(),
                    spm.getPropertyName(),
                    spm.getPropertyVisibility(),
                    FetchHints.ALL_INCLUDING_HIDDEN,
                    authorizations
            );
            if (property == null) {
                throw new GeException("Could not find property " + spm.getPropertyKey() + ":" + spm.getPropertyName());
            }

            Metadata metadata = Metadata.create(property.getMetadata());
            metadata.add(spm.getMetadataName(), spm.getNewValue(), spm.getMetadataVisibility());

            long newTimestamp = IncreasingTime.currentTimeMillis();
            inMemoryTableElement.appendAddPropertyMetadataMutation(
                    property.getKey(), property.getName(), metadata, property.getVisibility(), newTimestamp);
        }
    }

    protected StreamingPropertyValueRef saveStreamingPropertyValue(
            String elementId,
            String key,
            String name,
            Visibility visibility,
            long timestamp,
            StreamingPropertyValue value
    ) {
        return new InMemoryStreamingPropertyValueRef(value);
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        return authorizations.canRead(visibility);
    }

    @Override
    public void truncate() {
        this.verticesTable.clear();
        this.edgesTable.clear();
        getSearchIndex().truncate(this);
    }

    @Override
    public void drop() {
        this.verticesTable.clear();
        this.edgesTable.clear();
        getSearchIndex().drop(this);
    }

    protected void alterEdgeLabel(InMemoryTableEdge inMemoryTableEdge, long timestamp, String newEdgeLabel) {
        inMemoryTableEdge.appendAlterEdgeLabelMutation(timestamp, newEdgeLabel);
    }

    protected void alterConceptType(InMemoryTableVertex inMemoryTableVertex, long timestamp, String newConceptType) {
        inMemoryTableVertex.appendAlterConceptTypeMutation(timestamp, newConceptType);
    }

    protected void deleteProperty(
            InMemoryElement element,
            InMemoryTableElement inMemoryTableElement,
            String key,
            String name,
            Visibility visibility,
            Authorizations authorizations
    ) {
        Property property = inMemoryTableElement.getProperty(key, name, visibility, FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
        inMemoryTableElement.deleteProperty(key, name, visibility, authorizations);

        getSearchIndex().deleteProperty(this, element, PropertyDescriptor.fromProperty(property), authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeletePropertyEvent(this, element, property));
        }
    }

    private void refreshVertexInMemoryTableElement(Vertex vertex) {
        ((InMemoryVertex) vertex).setInMemoryTableElement(this.verticesTable.getTableElement(vertex.getId()));
    }

    public ImmutableSet<String> getExtendedDataTableNames(
            ElementType elementType,
            String elementId,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        return extendedDataTable.getTableNames(elementType, elementId, fetchHints, authorizations);
    }

    public Iterable<? extends ExtendedDataRow> getExtendedDataTable(
            ElementType elementType,
            String elementId,
            String tableName,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        return extendedDataTable.getTable(elementType, elementId, tableName, fetchHints, authorizations);
    }

    public void extendedData(
            Element element,
            ExtendedDataRowId rowId,
            ExtendedDataMutation extendedData,
            Authorizations authorizations
    ) {
        extendedDataTable.addData(rowId, extendedData.getColumnName(), extendedData.getKey(), extendedData.getValue(), extendedData.getTimestamp(), extendedData.getVisibility());
        getSearchIndex().addElementExtendedData(
                this,
                element,
                Collections.singleton(extendedData),
                authorizations
        );
        if (hasEventListeners()) {
            fireGraphEvent(new AddExtendedDataEvent(
                    this,
                    element,
                    rowId.getTableName(),
                    rowId.getRowId(),
                    extendedData.getColumnName(),
                    extendedData.getKey(),
                    extendedData.getValue(),
                    extendedData.getVisibility()
            ));
        }
    }

    @Override
    public void deleteExtendedDataRow(ExtendedDataRowId id, Authorizations authorizations) {
        List<ExtendedDataRow> rows = Lists.newArrayList(getExtendedData(Lists.newArrayList(id), authorizations));
        if (rows.size() > 1) {
            throw new GeException("Found too many extended data rows for id: " + id);
        }
        if (rows.size() != 1) {
            return;
        }

        this.extendedDataTable.remove(id);
        getSearchIndex().deleteExtendedData(this, id, authorizations);

        if (hasEventListeners()) {
            fireGraphEvent(new DeleteExtendedDataRowEvent(this, id));
        }
    }

    public void deleteExtendedData(
            InMemoryElement element,
            String tableName,
            String row,
            String columnName,
            String key,
            Visibility visibility,
            Authorizations authorizations
    ) {
        extendedDataTable.removeColumn(
                new ExtendedDataRowId(ElementType.getTypeFromElement(element), element.getId(), tableName, row),
                columnName,
                key,
                visibility
        );

        getSearchIndex().deleteExtendedData(this, element, tableName, row, columnName, key, visibility, authorizations);
        if (hasEventListeners()) {
            fireGraphEvent(new DeleteExtendedDataEvent(this, element, tableName, row, columnName, key));
        }
    }

    @Override
    public void flushGraph() {
        // no need to do anything here
    }

    @Override
    public Vertex getVertex(
            String vertexId, FetchHints fetchHints, Long endTime,
            Authorizations authorizations
    ) {
        validateAuthorizations(authorizations);

        InMemoryTableElement<InMemoryVertex> element = verticesTable.getTableElement(vertexId);
        if (element == null || !isIncludedInTimeSpan(element, fetchHints, endTime, authorizations)) {
            return null;
        } else {
            return element.createElement(this, fetchHints, endTime, authorizations);
        }
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        InMemoryTableElement<InMemoryEdge> element = edgesTable.getTableElement(edgeId);
        if (element == null || !isIncluded(element, fetchHints, authorizations)) {
            return null;
        } else {
            return element.createElement(this, fetchHints, endTime, authorizations);
        }
    }
}

