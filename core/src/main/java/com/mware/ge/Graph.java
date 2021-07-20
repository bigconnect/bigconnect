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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mware.ge.event.GraphEventListener;
import com.mware.ge.id.IdGenerator;
import com.mware.ge.id.IdentityNameSubstitutionStrategy;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.FilterIterable;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.LookAheadIterable;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.query.GraphQuery;
import com.mware.ge.query.MultiVertexQuery;
import com.mware.ge.query.SimilarToGraphQuery;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mware.ge.util.IterableUtils.count;
import static com.mware.ge.util.Preconditions.checkNotNull;

public interface Graph {
    /**
     * Adds a vertex to the graph. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param visibility     The visibility to assign to the new vertex.
     * @param authorizations The authorizations required to add and retrieve the new vertex.
     * @return The newly added vertex.
     */
    default Vertex addVertex(Visibility visibility, Authorizations authorizations, String conceptType) {
        return prepareVertex(visibility, conceptType).save(authorizations);
    }

    /**
     * Adds a vertex to the graph.
     *
     * @param vertexId       The id to assign the new vertex.
     * @param visibility     The visibility to assign to the new vertex.
     * @param authorizations The authorizations required to add and retrieve the new vertex.
     * @return The newly added vertex.
     */
    default Vertex addVertex(String vertexId, Visibility visibility, Authorizations authorizations, String conceptType) {
        return prepareVertex(vertexId, visibility, conceptType).save(authorizations);
    }

    /**
     * Adds the vertices to the graph.
     *
     * @param vertices       The vertices to add.
     * @param authorizations The authorizations required to add and retrieve the new vertex.
     * @return The vertices.
     */
    default Iterable<Vertex> addVertices(Iterable<ElementBuilder<Vertex>> vertices, Authorizations authorizations) {
        List<Vertex> addedVertices = new ArrayList<>();
        for (ElementBuilder<Vertex> vertexBuilder : vertices) {
            addedVertices.add(vertexBuilder.save(authorizations));
        }
        return addedVertices;
    }

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    default VertexBuilder prepareVertex(Visibility visibility, String conceptType) {
        return prepareVertex(getIdGenerator().nextId(), null, visibility, conceptType);
    }

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param timestamp  The timestamp of the vertex. null, to use the system generated time.
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    default VertexBuilder prepareVertex(Long timestamp, Visibility visibility, String conceptType) {
        return prepareVertex(getIdGenerator().nextId(), timestamp, visibility, conceptType);
    }

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation.
     *
     * @param vertexId   The id to assign the new vertex.
     * @param visibility The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    default VertexBuilder prepareVertex(String vertexId, Visibility visibility, String conceptType) {
        return prepareVertex(vertexId, null, visibility, conceptType);
    }

    /**
     * Prepare a vertex to be added to the graph. This method provides a way to build up a vertex with it's properties to be inserted
     * with a single operation.
     *
     * @param vertexId    The id to assign the new vertex.
     * @param conceptType The concept to assign the new vertex.
     * @param timestamp   The timestamp of the vertex.
     * @param visibility  The visibility to assign to the new vertex.
     * @return The vertex builder.
     */
    VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility, String conceptType);

    /**
     * Tests the existence of a vertex with the given authorizations.
     *
     * @param vertexId       The vertex id to check existence of.
     * @param authorizations The authorizations required to load the vertex.
     * @return True if vertex exists.
     */
    default boolean doesVertexExist(String vertexId, Authorizations authorizations) {
        return getVertex(vertexId, FetchHints.NONE, authorizations) != null;
    }

    /**
     * Get an element from the graph.
     *
     * @param elementId      The element id to retrieve from the graph.
     * @param authorizations The authorizations required to load the element.
     * @return The element if successful. null if the element is not found or the required authorizations were not provided.
     */
    default Element getElement(ElementId elementId, Authorizations authorizations) {
        return getElement(elementId, getDefaultFetchHints(), authorizations);
    }

    /**
     * Get an element from the graph.
     *
     * @param elementId      The element id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the element to fetch.
     * @param authorizations The authorizations required to load the element.
     * @return The vertex if successful. null if the element is not found or the required authorizations were not provided.
     */
    default Element getElement(ElementId elementId, FetchHints fetchHints, Authorizations authorizations) {
        if (elementId instanceof Element) {
            Element element = (Element) elementId;
            if (element.getFetchHints().hasFetchHints(fetchHints)) {
                return element;
            }
        }
        switch (elementId.getElementType()) {
            case VERTEX:
                return getVertex(elementId.getId(), fetchHints, authorizations);
            case EDGE:
                return getEdge(elementId.getId(), fetchHints, authorizations);
            default:
                throw new GeException("Unhandled element type: " + elementId.getElementType());
        }
    }

    /**
     * Deletes multiple elements
     *
     * @param elementIds     The element ids to delete
     * @param authorizations The authorizations required to delete the elements
     */
    default void deleteElements(Stream<? extends ElementId> elementIds, Authorizations authorizations) {
        elementIds.forEach(elementId -> deleteElement(elementId, authorizations));
    }

    /**
     * Deletes an element
     *
     * @param elementId      The element to delete
     * @param authorizations The authorizations required to delete the element
     */
    default void deleteElement(ElementId elementId, Authorizations authorizations) {
        checkNotNull(elementId, "element is required");
        switch (elementId.getElementType()) {
            case VERTEX:
                if (elementId instanceof Vertex) {
                    deleteVertex((Vertex) elementId, authorizations);
                } else {
                    deleteVertex(elementId.getId(), authorizations);
                }
                break;
            case EDGE:
                if (elementId instanceof Edge) {
                    deleteEdge((Edge) elementId, authorizations);
                } else {
                    deleteEdge(elementId.getId(), authorizations);
                }
                break;
            default:
                throw new GeException("Unhandled element type: " + elementId.getElementType());
        }
    }

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId       The vertex id to retrieve from the graph.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Vertex getVertex(String vertexId, Authorizations authorizations) {
        return getVertex(vertexId, getDefaultFetchHints(), authorizations);
    }

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId       The vertex id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Vertex getVertex(String vertexId, FetchHints fetchHints, Authorizations authorizations) {
        return getVertex(vertexId, fetchHints, null, authorizations);
    }

    /**
     * Get a vertex from the graph.
     *
     * @param vertexId       The vertex id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
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

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, Authorizations authorizations) {
        return getVerticesWithPrefix(vertexIdPrefix, getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Authorizations authorizations) {
        return getVerticesWithPrefix(vertexIdPrefix, fetchHints, null, authorizations);
    }

    /**
     * Gets vertices from the graph given the prefix.
     *
     * @param vertexIdPrefix The prefix of the vertex ids to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertex if successful. null if the vertex is not found or the required authorizations were not provided.
     */
    default Iterable<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<Vertex> vertices = getVertices(fetchHints, endTime, authorizations);
        return new FilterIterable<Vertex>(vertices) {
            @Override
            protected boolean isIncluded(Vertex v) {
                return v.getId().startsWith(vertexIdPrefix);
            }
        };
    }

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertices in the range.
     */
    default Iterable<Vertex> getVerticesInRange(IdRange idRange, Authorizations authorizations) {
        return getVerticesInRange(idRange, getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertices in the range.
     */
    default Iterable<Vertex> getVerticesInRange(IdRange idRange, FetchHints fetchHints, Authorizations authorizations) {
        return getVerticesInRange(idRange, fetchHints, null, authorizations);
    }

    /**
     * Gets vertices from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The vertices in the range.
     */
    default Iterable<Vertex> getVerticesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<Vertex> vertices = getVertices(fetchHints, endTime, authorizations);
        return new FilterIterable<Vertex>(vertices) {
            @Override
            protected boolean isIncluded(Vertex v) {
                return idRange.isInRange(v.getId());
            }
        };
    }

    /**
     * Gets all vertices on the graph.
     *
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Iterable<Vertex> getVertices(Authorizations authorizations) {
        return getVertices(getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets all vertices on the graph.
     *
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Iterable<Vertex> getVertices(FetchHints fetchHints, Authorizations authorizations) {
        return getVertices(fetchHints, null, authorizations);
    }

    Iterable<String> getVertexIds(Authorizations authorizations);

    /**
     * Gets all vertices on the graph.
     *
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    Iterable<Vertex> getVertices(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Tests the existence of vertices with the given authorizations.
     *
     * @param ids            The vertex ids to check existence of.
     * @param authorizations The authorizations required to load the vertices.
     * @return Map of ids to exists status.
     */
    default Map<String, Boolean> doVerticesExist(Iterable<String> ids, Authorizations authorizations) {
        Map<String, Boolean> results = new HashMap<>();
        for (String id : ids) {
            results.put(id, false);
        }
        for (Vertex vertex : getVertices(ids, FetchHints.NONE, authorizations)) {
            results.put(vertex.getId(), true);
        }
        return results;
    }

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids            The ids of the vertices to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Iterable<Vertex> getVertices(Iterable<String> ids, Authorizations authorizations) {
        return getVertices(ids, getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids            The ids of the vertices to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Iterable<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations) {
        return getVertices(ids, fetchHints, null, authorizations);
    }

    /**
     * Gets all vertices matching the given ids on the graph. The order of
     * the returned vertices is not guaranteed {@link Graph#getVerticesInOrder(Iterable, Authorizations)}.
     * Vertices are not kept in memory during the iteration.
     *
     * @param ids            The ids of the vertices to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default Iterable<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
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

    /**
     * Gets all vertices matching the given ids on the graph. This method is similar to
     * {@link Graph#getVertices(Iterable, Authorizations)}
     * but returns the vertices in the order that you passed in the ids. This requires loading
     * all the vertices in memory to sort them.
     *
     * @param ids            The ids of the vertices to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default List<Vertex> getVerticesInOrder(Iterable<String> ids, Authorizations authorizations) {
        return getVerticesInOrder(ids, getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets all vertices matching the given ids on the graph. This method is similar to
     * {@link Graph#getVertices(Iterable, Authorizations)}
     * but returns the vertices in the order that you passed in the ids. This requires loading
     * all the vertices in memory to sort them.
     *
     * @param ids            The ids of the vertices to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return An iterable of all the vertices.
     */
    default List<Vertex> getVerticesInOrder(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations) {
        final List<String> vertexIds = IterableUtils.toList(ids);
        List<Vertex> vertices = IterableUtils.toList(getVertices(vertexIds, authorizations));
        vertices.sort((v1, v2) -> {
            Integer i1 = vertexIds.indexOf(v1.getId());
            Integer i2 = vertexIds.indexOf(v2.getId());
            return i1.compareTo(i2);
        });
        return vertices;
    }

    /**
     * Permanently deletes a vertex from the graph.
     *
     * @param vertex         The vertex to delete.
     * @param authorizations The authorizations required to delete the vertex.
     */
    void deleteVertex(Vertex vertex, Authorizations authorizations);

    /**
     * Permanently deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to delete.
     * @param authorizations The authorizations required to delete the vertex.
     */
    default void deleteVertex(String vertexId, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to delete with id: " + vertexId);
        deleteVertex(vertex, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    default void softDeleteVertex(Vertex vertex, Authorizations authorizations) {
        softDeleteVertex(vertex, null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertex         The vertex to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    void softDeleteVertex(Vertex vertex, Long timestamp, Authorizations authorizations);

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    default void softDeleteVertex(String vertexId, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to soft delete with id: " + vertexId);
        softDeleteVertex(vertex, null, authorizations);
    }

    /**
     * Soft deletes a vertex from the graph.
     *
     * @param vertexId       The vertex id to soft delete.
     * @param authorizations The authorizations required to soft delete the vertex.
     */
    default void softDeleteVertex(String vertexId, Long timestamp, Authorizations authorizations) {
        Vertex vertex = getVertex(vertexId, authorizations);
        checkNotNull(vertex, "Could not find vertex to soft delete with id: " + vertexId);
        softDeleteVertex(vertex, timestamp, authorizations);
    }

    /**
     * Adds an edge between two vertices. The id of the new vertex will be generated using an IdGenerator.
     *
     * @param outVertex      The source vertex. The "out" side of the edge.
     * @param inVertex       The destination vertex. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     */
    default Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(outVertex, inVertex, label, visibility).save(authorizations);
    }

    /**
     * Adds an edge between two vertices.
     *
     * @param edgeId         The id to assign the new edge.
     * @param outVertex      The source vertex. The "out" side of the edge.
     * @param inVertex       The destination vertex. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     */
    default Edge addEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertex, inVertex, label, visibility).save(authorizations);
    }

    /**
     * Adds an edge between two vertices.
     *
     * @param outVertexId    The source vertex id. The "out" side of the edge.
     * @param inVertexId     The destination vertex id. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     */
    default Edge addEdge(String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(outVertexId, inVertexId, label, visibility).save(authorizations);
    }

    /**
     * Adds an edge between two vertices.
     *
     * @param edgeId         The id to assign the new edge.
     * @param outVertexId    The source vertex id. The "out" side of the edge.
     * @param inVertexId     The destination vertex id. The "in" side of the edge.
     * @param label          The label to assign to the edge. eg knows, works at, etc.
     * @param visibility     The visibility to assign to the new edge.
     * @param authorizations The authorizations required to add and retrieve the new edge.
     * @return The newly created edge.
     */
    default Edge addEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility, Authorizations authorizations) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, visibility).save(authorizations);
    }

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation. The id of the new edge will be generated using an IdGenerator.
     *
     * @param outVertex  The source vertex. The "out" side of the edge.
     * @param inVertex   The destination vertex. The "in" side of the edge.
     * @param label      The label to assign to the edge. eg knows, works at, etc.
     * @param visibility The visibility to assign to the new edge.
     * @return The edge builder.
     */
    default EdgeBuilder prepareEdge(Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(getIdGenerator().nextId(), outVertex, inVertex, label, visibility);
    }

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param edgeId     The id to assign the new edge.
     * @param outVertex  The source vertex. The "out" side of the edge.
     * @param inVertex   The destination vertex. The "in" side of the edge.
     * @param label      The label to assign to the edge. eg knows, works at, etc.
     * @param visibility The visibility to assign to the new edge.
     * @return The edge builder.
     */
    default EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertex, inVertex, label, null, visibility);
    }

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param edgeId     The id to assign the new edge.
     * @param outVertex  The source vertex. The "out" side of the edge.
     * @param inVertex   The destination vertex. The "in" side of the edge.
     * @param label      The label to assign to the edge. eg knows, works at, etc.
     * @param timestamp  The timestamp of the edge.
     * @param visibility The visibility to assign to the new edge.
     * @return The edge builder.
     */
    EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility);

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param outVertexId The source vertex id. The "out" side of the edge.
     * @param inVertexId  The destination vertex id. The "in" side of the edge.
     * @param label       The label to assign to the edge. eg knows, works at, etc.
     * @param visibility  The visibility to assign to the new edge.
     * @return The edge builder.
     */
    default EdgeBuilderByVertexId prepareEdge(String outVertexId, String inVertexId, String label, Visibility visibility) {
        return prepareEdge(getIdGenerator().nextId(), outVertexId, inVertexId, label, visibility);
    }

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param edgeId      The id to assign the new edge.
     * @param outVertexId The source vertex id. The "out" side of the edge.
     * @param inVertexId  The destination vertex id. The "in" side of the edge.
     * @param label       The label to assign to the edge. eg knows, works at, etc.
     * @param visibility  The visibility to assign to the new edge.
     * @return The edge builder.
     */
    default EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Visibility visibility) {
        return prepareEdge(edgeId, outVertexId, inVertexId, label, null, visibility);
    }

    /**
     * Prepare an edge to be added to the graph. This method provides a way to build up an edge with it's properties to be inserted
     * with a single operation.
     *
     * @param edgeId      The id to assign the new edge.
     * @param outVertexId The source vertex id. The "out" side of the edge.
     * @param inVertexId  The destination vertex id. The "in" side of the edge.
     * @param label       The label to assign to the edge. eg knows, works at, etc.
     * @param timestamp   The timestamp of the edge.
     * @param visibility  The visibility to assign to the new edge.
     * @return The edge builder.
     */
    EdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility);

    /**
     * Tests the existence of a edge with the given authorizations.
     *
     * @param edgeId         The edge id to check existence of.
     * @param authorizations The authorizations required to load the edge.
     * @return True if edge exists.
     */
    default boolean doesEdgeExist(String edgeId, Authorizations authorizations) {
        return getEdge(edgeId, FetchHints.NONE, authorizations) != null;
    }

    /**
     * Get an edge from the graph.
     *
     * @param edgeId         The edge id to retrieve from the graph.
     * @param authorizations The authorizations required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    default Edge getEdge(String edgeId, Authorizations authorizations) {
        return getEdge(edgeId, getDefaultFetchHints(), authorizations);
    }

    /**
     * Get an edge from the graph.
     *
     * @param edgeId         The edge id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param authorizations The authorizations required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    default Edge getEdge(String edgeId, FetchHints fetchHints, Authorizations authorizations) {
        return getEdge(edgeId, fetchHints, null, authorizations);
    }

    /**
     * Get an edge from the graph.
     *
     * @param edgeId         The edge id to retrieve from the graph.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edge.
     * @return The edge if successful. null if the edge is not found or the required authorizations were not provided.
     */
    default Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
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

    /**
     * Gets all edges on the graph.
     *
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    default Iterable<Edge> getEdges(Authorizations authorizations) {
        return getEdges(getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets all edges on the graph.
     *
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    default Iterable<Edge> getEdges(FetchHints fetchHints, Authorizations authorizations) {
        return getEdges(fetchHints, null, authorizations);
    }

    /**
     * Gets all edges on the graph.
     *
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    Iterable<Edge> getEdges(FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param authorizations The authorizations required to load the vertex.
     * @return The edges in the range.
     */
    default Iterable<Edge> getEdgesInRange(IdRange idRange, Authorizations authorizations) {
        return getEdgesInRange(idRange, getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param authorizations The authorizations required to load the vertex.
     * @return The edges in the range.
     */
    default Iterable<Edge> getEdgesInRange(IdRange idRange, FetchHints fetchHints, Authorizations authorizations) {
        return getEdgesInRange(idRange, fetchHints, null, authorizations);
    }

    /**
     * Gets edges from the graph in the given range.
     *
     * @param idRange        The range of ids to get.
     * @param fetchHints     Hint at what parts of the vertex to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the vertex.
     * @return The edges in the range.
     */
    default Iterable<Edge> getEdgesInRange(IdRange idRange, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        Iterable<Edge> edges = getEdges(fetchHints, endTime, authorizations);
        return new FilterIterable<Edge>(edges) {
            @Override
            protected boolean isIncluded(Edge e) {
                return idRange.isInRange(e.getId());
            }
        };
    }

    /**
     * Filters a collection of edge ids by the authorizations of that edge, properties, etc. If
     * any of the filtered items match that edge id will be included.
     *
     * @param edgeIds              The edge ids to filter on.
     * @param authorizationToMatch The authorization to look for
     * @param filters              The parts of the edge to filter on
     * @param authorizations       The authorization to find the edges with
     * @return The filtered down list of edge ids
     */
    Iterable<String> filterEdgeIdsByAuthorization(Iterable<String> edgeIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations);

    /**
     * Filters a collection of vertex ids by the authorizations of that vertex, properties, etc. If
     * any of the filtered items match that vertex id will be included.
     *
     * @param vertexIds            The vertex ids to filter on.
     * @param authorizationToMatch The authorization to look for
     * @param filters              The parts of the edge to filter on
     * @param authorizations       The authorization to find the edges with
     * @return The filtered down list of vertex ids
     */
    Iterable<String> filterVertexIdsByAuthorization(Iterable<String> vertexIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations);

    /**
     * Tests the existence of edges with the given authorizations.
     *
     * @param ids            The edge ids to check existence of.
     * @param authorizations The authorizations required to load the edges.
     * @return Maps of ids to exists status.
     */
    default Map<String, Boolean> doEdgesExist(Iterable<String> ids, Authorizations authorizations) {
        return doEdgesExist(ids, null, authorizations);
    }

    /**
     * Tests the existence of edges with the given authorizations.
     *
     * @param ids            The edge ids to check existence of.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edges.
     * @return Maps of ids to exists status.
     */
    default Map<String, Boolean> doEdgesExist(Iterable<String> ids, Long endTime, Authorizations authorizations) {
        Map<String, Boolean> results = new HashMap<>();
        for (String id : ids) {
            results.put(id, false);
        }
        for (Edge edge : getEdges(ids, FetchHints.NONE, endTime, authorizations)) {
            results.put(edge.getId(), true);
        }
        return results;
    }

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids            The ids of the edges to get.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    default Iterable<Edge> getEdges(Iterable<String> ids, Authorizations authorizations) {
        return getEdges(ids, getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids            The ids of the edges to get.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    default Iterable<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Authorizations authorizations) {
        return getEdges(ids, fetchHints, null, authorizations);
    }

    /**
     * Gets all edges on the graph matching the given ids.
     *
     * @param ids            The ids of the edges to get.
     * @param fetchHints     Hint at what parts of the edge to fetch.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edge.
     * @return An iterable of all the edges.
     */
    default Iterable<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
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

    /**
     * Use {@link #findRelatedEdgeIds(Iterable, Authorizations)}
     */
    @Deprecated
    default Iterable<String> findRelatedEdges(Iterable<String> vertexIds, Authorizations authorizations) {
        return findRelatedEdgeIds(vertexIds, authorizations);
    }

    /**
     * Use {@link #findRelatedEdgeIds(Iterable, Long, Authorizations)}
     */
    @Deprecated
    default Iterable<String> findRelatedEdges(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        return findRelatedEdgeIds(vertexIds, endTime, authorizations);
    }

    /**
     * Given a list of vertices, find all the edge ids that connect them.
     *
     * @param verticesIterable The list of vertices.
     * @param authorizations   The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    default Iterable<String> findRelatedEdgeIdsForVertices(Iterable<Vertex> verticesIterable, Authorizations authorizations) {
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

    /**
     * Given a list of vertex ids, find all the edge ids that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param authorizations The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    default Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Authorizations authorizations) {
        return findRelatedEdgeIds(vertexIds, null, authorizations);
    }

    /**
     * Given a list of vertex ids, find all the edge ids that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edges.
     * @return An iterable of all the edge ids between any two vertices.
     */
    default Iterable<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        FetchHints fetchHints = new FetchHintsBuilder()
                .setIncludeOutEdgeRefs(true)
                .build();
        return findRelatedEdgeIdsForVertices(getVertices(vertexIds, fetchHints, endTime, authorizations), authorizations);
    }

    /**
     * Given a list of vertices, find all the edges that connect them.
     *
     * @param verticesIterable The list of vertices.
     * @param authorizations   The authorizations required to load the edges.
     * @return Summary information about the related edges.
     */
    default Iterable<RelatedEdge> findRelatedEdgeSummaryForVertices(Iterable<Vertex> verticesIterable, Authorizations authorizations) {
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

    /**
     * Given a list of vertex ids, find all the edges that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param authorizations The authorizations required to load the edges.
     * @return Summary information about the related edges.
     */
    default Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Authorizations authorizations) {
        return findRelatedEdgeSummary(vertexIds, null, authorizations);
    }

    /**
     * Given a list of vertex ids, find all the edges that connect them.
     *
     * @param vertexIds      The list of vertex ids.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations required to load the edges.
     * @return Summary information about the related edges.
     */
    default Iterable<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, Authorizations authorizations) {
        FetchHints fetchHints = new FetchHintsBuilder()
                .setIncludeOutEdgeRefs(true)
                .build();
        return findRelatedEdgeSummaryForVertices(getVertices(vertexIds, fetchHints, endTime, authorizations), authorizations);
    }

    /**
     * Permanently deletes an edge from the graph.
     *
     * @param edge           The edge to delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    void deleteEdge(Edge edge, Authorizations authorizations);

    /**
     * Permanently deletes an edge from the graph. This method requires fetching the edge before deletion.
     *
     * @param edgeId         The edge id of the edge to delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    default void deleteEdge(String edgeId, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to delete with id: " + edgeId);
        deleteEdge(edge, authorizations);
    }

    /**
     * Soft deletes an edge from the graph.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    default void softDeleteEdge(Edge edge, Authorizations authorizations) {
        softDeleteEdge(edge, null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph.
     *
     * @param edge           The edge to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    void softDeleteEdge(Edge edge, Long timestamp, Authorizations authorizations);

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    default void softDeleteEdge(String edgeId, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to soft delete with id: " + edgeId);
        softDeleteEdge(edge, null, authorizations);
    }

    /**
     * Soft deletes an edge from the graph. This method requires fetching the edge before soft deletion.
     *
     * @param edgeId         The edge id of the vertex to soft delete from the graph.
     * @param authorizations The authorizations required to delete the edge.
     */
    default void softDeleteEdge(String edgeId, Long timestamp, Authorizations authorizations) {
        Edge edge = getEdge(edgeId, authorizations);
        checkNotNull(edge, "Could not find edge to soft delete with id: " + edgeId);
        softDeleteEdge(edge, timestamp, authorizations);
    }

    /**
     * Creates a query builder object used to query the graph.
     *
     * @param queryString    The string to search for in the text of an element. This will search all fields for the given text.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    GraphQuery query(String queryString, Authorizations authorizations);

    /**
     * Creates a query builder object used to query the graph.
     *
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    GraphQuery query(Authorizations authorizations);

    /**
     * Creates a query builder object used to query a list of vertices.
     *
     * @param vertexIds      The vertex ids to query.
     * @param queryString    The string to search for in the text of an element. This will search all fields for the given text.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    MultiVertexQuery query(String[] vertexIds, String queryString, Authorizations authorizations);

    /**
     * Creates a query builder object used to query a list of vertices.
     *
     * @param vertexIds      The vertex ids to query.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    MultiVertexQuery query(String[] vertexIds, Authorizations authorizations);

    /**
     * Returns true if this graph supports similar to text queries.
     */
    boolean isQuerySimilarToTextSupported();

    /**
     * Creates a query builder object that finds all vertices similar to the given text for the specified fields.
     * This could be implemented similar to the ElasticSearch more like this query.
     *
     * @param fields         The fields to match against.
     * @param text           The text to find similar to.
     * @param authorizations The authorizations required to load the elements.
     * @return A query builder object.
     */
    SimilarToGraphQuery querySimilarTo(String[] fields, String text, Authorizations authorizations);

    /**
     * Flushes any pending mutations to the graph.
     */
    void flush();

    /**
     * Cleans up or disconnects from the underlying storage.
     */
    void shutdown();

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId The source vertex id to start the search from.
     * @param destVertexId   The destination vertex id to get to.
     * @param maxHops        The maximum number of hops to make before giving up.
     * @param authorizations The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     * @deprecated Use {@link #findPaths(FindPathOptions, Authorizations)}
     */
    @Deprecated
    default Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, Authorizations authorizations) {
        return findPaths(sourceVertexId, destVertexId, null, maxHops, authorizations);
    }

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId The source vertex id to start the search from.
     * @param destVertexId   The destination vertex id to get to.
     * @param labels         Edge labels
     * @param maxHops        The maximum number of hops to make before giving up.
     * @param authorizations The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     * @deprecated Use {@link #findPaths(FindPathOptions, Authorizations)}
     */
    @Deprecated
    default Iterable<Path> findPaths(String sourceVertexId, String destVertexId, String[] labels, int maxHops, Authorizations authorizations) {
        return findPaths(sourceVertexId, destVertexId, labels, maxHops, null, authorizations);
    }

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId   The source vertex id to start the search from.
     * @param destVertexId     The destination vertex id to get to.
     * @param maxHops          The maximum number of hops to make before giving up.
     * @param progressCallback Callback used to report progress.
     * @param authorizations   The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     * @deprecated Use {@link #findPaths(FindPathOptions, Authorizations)}
     */
    @Deprecated
    default Iterable<Path> findPaths(String sourceVertexId, String destVertexId, int maxHops, ProgressCallback progressCallback, Authorizations authorizations) {
        return findPaths(sourceVertexId, destVertexId, null, maxHops, progressCallback, authorizations);
    }

    /**
     * Finds all paths between two vertices.
     *
     * @param sourceVertexId   The source vertex id to start the search from.
     * @param destVertexId     The destination vertex id to get to.
     * @param labels           Edge labels
     * @param maxHops          The maximum number of hops to make before giving up.
     * @param progressCallback Callback used to report progress.
     * @param authorizations   The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     * @deprecated Use {@link #findPaths(FindPathOptions, Authorizations)}
     */
    @Deprecated
    default Iterable<Path> findPaths(String sourceVertexId, String destVertexId, String[] labels, int maxHops, ProgressCallback progressCallback, Authorizations authorizations) {
        FindPathOptions options = new FindPathOptions(sourceVertexId, destVertexId, maxHops);
        options.setLabels(labels);
        options.setProgressCallback(progressCallback);
        return findPaths(options, authorizations);
    }

    /**
     * Finds all paths between two vertices.
     *
     * @param options        Find path options
     * @param authorizations The authorizations required to load all edges and vertices.
     * @return An Iterable of lists of paths.
     */
    Iterable<Path> findPaths(FindPathOptions options, Authorizations authorizations);

    /**
     * Gets the id generator used by this graph to create ids.
     *
     * @return the id generator.
     */
    IdGenerator getIdGenerator();

    /**
     * Given an authorization is the visibility object valid.
     *
     * @param visibility     The visibility you want to check.
     * @param authorizations The given authorizations.
     * @return true if the visibility is valid given an authorization, else return false.
     */
    boolean isVisibilityValid(Visibility visibility, Authorizations authorizations);

    /**
     * Reindex all vertices and edges.
     *
     * @param authorizations authorizations used to query for the data to reindex.
     */
    void reindex(Authorizations authorizations);

    /**
     * Sets metadata on the graph.
     *
     * @param key   The key to the metadata.
     * @param value The value to set.
     */
    void setMetadata(String key, Object value);

    /**
     * Removes metadata from graph
     *
     * @param key
     */
    void removeMetadata(String key);

    /**
     * Gets metadata from the graph.
     *
     * @param key The key to the metadata.
     * @return The metadata value, or null.
     */
    Object getMetadata(String key);

    /**
     * Force a reload of graph metadata.
     */
    void reloadMetadata();

    /**
     * Gets all metadata.
     *
     * @return Iterable of all metadata.
     */
    Iterable<GraphMetadataEntry> getMetadata();

    /**
     * Gets all metadata with the given prefix.
     */
    Iterable<GraphMetadataEntry> getMetadataWithPrefix(String prefix);

    /**
     * Determine if field boost is support. That is can you change the boost at a field level to give higher priority.
     */
    boolean isFieldBoostSupported();

    /**
     * Clears all data from the graph.
     */
    void truncate();

    /**
     * Drops all tables. The graph needs to be reinitialized after this operation.
     */
    void drop();

    /**
     * Gets the granularity of the search index {@link SearchIndexSecurityGranularity}
     */
    SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    /**
     * Adds a graph event listener that will be called when graph events occur.
     */
    void addGraphEventListener(GraphEventListener graphEventListener);

    /**
     * Removes a graph event listener
     *
     * @param graphEventListener The listener to remove
     */
    void removeGraphEventListener(GraphEventListener graphEventListener);

    /**
     * Marks a vertex as hidden for a given visibility.
     *
     * @param vertex         The vertex to mark hidden.
     * @param visibility     The visibility string under which this vertex is hidden.
     *                       This visibility can be a superset of the vertex visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     */
    void markVertexHidden(Vertex vertex, Visibility visibility, Authorizations authorizations);

    /**
     * Marks a vertex as visible for a given visibility, effectively undoing markVertexHidden.
     *
     * @param vertex         The vertex to mark visible.
     * @param visibility     The visibility string under which this vertex is now visible.
     * @param authorizations The authorizations used.
     */
    void markVertexVisible(Vertex vertex, Visibility visibility, Authorizations authorizations);

    /**
     * Marks an edge as hidden for a given visibility.
     *
     * @param edge           The edge to mark hidden.
     * @param visibility     The visibility string under which this edge is hidden.
     *                       This visibility can be a superset of the edge visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     */
    void markEdgeHidden(Edge edge, Visibility visibility, Authorizations authorizations);

    /**
     * Marks an edge as visible for a given visibility, effectively undoing markEdgeHidden.
     *
     * @param edge           The edge to mark visible.
     * @param visibility     The visibility string under which this edge is now visible.
     * @param authorizations The authorizations used.
     */
    void markEdgeVisible(Edge edge, Visibility visibility, Authorizations authorizations);

    /**
     * Creates an authorizations object.
     *
     * @param auths The authorizations granted.
     * @return A new authorizations object
     */
    Authorizations createAuthorizations(String... auths);

    /**
     * Creates an authorizations object.
     *
     * @param auths The authorizations granted.
     * @return A new authorizations object
     */
    default Authorizations createAuthorizations(Collection<String> auths) {
        checkNotNull(auths, "auths cannot be null");
        return createAuthorizations(auths.toArray(new String[0]));
    }

    /**
     * Creates an authorizations object combining auths and additionalAuthorizations.
     *
     * @param auths                    The authorizations granted.
     * @param additionalAuthorizations additional authorizations
     * @return A new authorizations object
     */
    Authorizations createAuthorizations(Authorizations auths, String... additionalAuthorizations);

    /**
     * Creates an authorizations object combining auths and additionalAuthorizations.
     *
     * @param auths                    The authorizations granted.
     * @param additionalAuthorizations additional authorizations
     * @return A new authorizations object
     */
    default Authorizations createAuthorizations(Authorizations auths, Collection<String> additionalAuthorizations) {
        return createAuthorizations(auths, additionalAuthorizations.toArray(new String[0]));
    }

    /**
     * Gets the number of times a property with a given value occurs on vertices
     *
     * @param propertyName   The name of the property to find
     * @param authorizations The authorizations to use to find the property
     * @return The results
     */
    default Map<Object, Long> getVertexPropertyCountByValue(String propertyName, Authorizations authorizations) {
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

    /**
     * Gets a count of the number of vertices in the system.
     */
    default long getVertexCount(Authorizations authorizations) {
        return count(getVertices(authorizations));
    }

    /**
     * Gets a count of the number of edges in the system.
     */
    default long getEdgeCount(Authorizations authorizations) {
        return count(getEdges(authorizations));
    }

    /**
     * Save a pre-made property definition.
     *
     * @param propertyDefinition the property definition to save.
     */
    void savePropertyDefinition(PropertyDefinition propertyDefinition);

    /**
     * Creates a defines property builder. This is typically used by the indexer to give it hints on how it should index a property.
     *
     * @param propertyName The name of the property to define.
     */
    default DefinePropertyBuilder defineProperty(String propertyName) {
        return new DefinePropertyBuilder(propertyName) {
            @Override
            public PropertyDefinition define() {
                PropertyDefinition propertyDefinition = super.define();
                savePropertyDefinition(propertyDefinition);
                return propertyDefinition;
            }
        };
    }

    /**
     * Determine if a property is already defined
     */
    boolean isPropertyDefined(String propertyName);

    /**
     * Gets the property definition for the given name.
     *
     * @param propertyName name of the property
     * @return the property definition if found. null otherwise.
     */
    PropertyDefinition getPropertyDefinition(String propertyName);

    /**
     * Removes the property with the given name
     *
     * @param propertyName
     */
    void removePropertyDefinition(String propertyName);

    /**
     * Gets all property definitions.
     *
     * @return all property definitions.
     */
    Collection<PropertyDefinition> getPropertyDefinitions();

    /**
     * Saves multiple mutations with a single call.
     *
     * @param mutations      the mutations to save
     * @param authorizations the authorizations used during save
     * @return the elements which were saved
     */
    Iterable<Element> saveElementMutations(
            Iterable<ElementMutation<? extends Element>> mutations,
            Authorizations authorizations
    );

    /**
     * Opens multiple StreamingPropertyValue input streams at once. This can have performance benefits by
     * reducing the number of queries to the underlying data source.
     *
     * @param streamingPropertyValues list of StreamingPropertyValues to get input streams for
     * @return InputStreams in the same order as the input list
     */
    default List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        return streamingPropertyValues.stream()
                .map(StreamingPropertyValue::getInputStream)
                .collect(Collectors.toList());
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param ids            The ids of the rows to get.
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    default Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, Authorizations authorizations) {
        return getExtendedData(ids, FetchHints.ALL, authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param ids            The ids of the rows to get.
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    Iterable<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets the specified extended data row.
     *
     * @param id             The id of the row to get.
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    default ExtendedDataRow getExtendedData(ExtendedDataRowId id, Authorizations authorizations) {
        ArrayList<ExtendedDataRow> rows = Lists.newArrayList(getExtendedData(Lists.newArrayList(id), authorizations));
        if (rows.size() == 0) {
            return null;
        }
        if (rows.size() == 1) {
            return rows.get(0);
        }
        throw new GeException("Expected 0 or 1 rows found " + rows.size());
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementType    The type of element to get the rows from
     * @param elementId      The element id to get the rows from
     * @param tableName      The name of the table within the element to get the rows from
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    default Iterable<ExtendedDataRow> getExtendedData(
            ElementType elementType,
            String elementId,
            String tableName,
            Authorizations authorizations
    ) {
        return getExtendedData(elementType, elementId, tableName, FetchHints.ALL, authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementId      The element id to get the rows from
     * @param tableName      The name of the table within the element to get the rows from
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    default Iterable<ExtendedDataRow> getExtendedData(
            ElementId elementId,
            String tableName,
            Authorizations authorizations
    ) {
        return getExtendedData(elementId, tableName, FetchHints.ALL, authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementType    The type of element to get the rows from
     * @param elementId      The element id to get the rows from
     * @param tableName      The name of the table within the element to get the rows from
     * @param fetchHints     Fetch hints to filter extended data
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    default Iterable<ExtendedDataRow> getExtendedData(
            ElementType elementType,
            String elementId,
            String tableName,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        return getExtendedData(ElementId.create(elementType, elementId), tableName, fetchHints, authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementId      The element id to get the rows from
     * @param tableName      The name of the table within the element to get the rows from
     * @param fetchHints     Fetch hints to filter extended data
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    default Iterable<ExtendedDataRow> getExtendedData(
            ElementId elementId,
            String tableName,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        return getExtendedDataForElements(Lists.newArrayList(elementId), tableName, fetchHints, authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementIds     The element ids of the elements to get the rows from
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    default Iterable<ExtendedDataRow> getExtendedDataForElements(
            Iterable<? extends ElementId> elementIds,
            Authorizations authorizations
    ) {
        return getExtendedDataForElements(elementIds, FetchHints.ALL, authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementIds     The element ids of the elements to get the rows from
     * @param tableName      The name of the table within the element to get the rows from
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    default Iterable<ExtendedDataRow> getExtendedDataForElements(
            Iterable<? extends ElementId> elementIds,
            String tableName,
            Authorizations authorizations
    ) {
        return getExtendedDataForElements(elementIds, tableName, FetchHints.ALL, authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementIds     The element ids of the elements to get the rows from
     * @param fetchHints     Fetch hints to filter extended data
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    default Iterable<ExtendedDataRow> getExtendedDataForElements(
            Iterable<? extends ElementId> elementIds,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        return getExtendedDataForElements(elementIds, null, fetchHints, authorizations);
    }

    /**
     * Gets the specified extended data rows.
     *
     * @param elementIds     The element ids of the elements to get the rows from
     * @param tableName      The name of the table within the element to get the rows from
     * @param fetchHints     Fetch hints to filter extended data
     * @param authorizations The authorizations used to get the rows
     * @return Rows
     */
    Iterable<ExtendedDataRow> getExtendedDataForElements(
            Iterable<? extends ElementId> elementIds,
            String tableName,
            FetchHints fetchHints,
            Authorizations authorizations
    );

    /**
     * Gets extended data rows from the graph in the given range.
     *
     * @param elementType    The type of element to get the rows from
     * @param elementIdRange The range of element ids to get extended data rows for.
     * @param authorizations The authorizations required to load the vertex.
     * @return The extended data rows for the element ids in the range.
     */
    Iterable<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, IdRange elementIdRange, Authorizations authorizations);

    /**
     * Deletes an extended data row
     */
    void deleteExtendedDataRow(ExtendedDataRowId id, Authorizations authorizations);

    /**
     * The default fetch hints to use if none are provided
     */
    FetchHints getDefaultFetchHints();

    /**
     * Visits all elements on the graph
     */
    default void visitElements(GraphVisitor graphVisitor, Authorizations authorizations) {
        visitVertices(graphVisitor, authorizations);
        visitEdges(graphVisitor, authorizations);
    }

    /**
     * Visits all vertices on the graph
     */
    default void visitVertices(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getVertices(authorizations), graphVisitor);
    }

    /**
     * Visits all edges on the graph
     */
    default void visitEdges(GraphVisitor graphVisitor, Authorizations authorizations) {
        visit(getEdges(authorizations), graphVisitor);
    }

    /**
     * Visits elements using the supplied elements and visitor
     */
    default void visit(Iterable<? extends Element> elements, GraphVisitor visitor) {
        for (Element element : elements) {
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
        }
    }

    void dumpGraph();

    /**
     * Gets the metrics registry to record internal GraphEngine metrics
     */
    GeMetricRegistry getMetricsRegistry();

    default NameSubstitutionStrategy getNameSubstitutionStrategy() {
        return new IdentityNameSubstitutionStrategy();
    }

    void ensurePropertyDefined(String name, Value value);
}
