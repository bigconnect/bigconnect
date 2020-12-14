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

import com.mware.ge.mutation.ExistingVertexMutation;
import com.mware.ge.query.VertexQuery;

public interface Vertex extends Element, VertexElementLocation {
    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, Authorizations authorizations);

    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all edges attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations);

    /**
     * Gets all edges with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets all edges with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching the given label.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching the given label.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param label          The edge label to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets the connected edge ids.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids
     */
    Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching any of the given labels.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets all edges between this vertex and another vertex matching any of the given labels.
     *
     * @param otherVertex    The other vertex.
     * @param direction      The direction of edges to find relative to this vertex.
     * @param labels         An array of edge labels to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edges.
     */
    Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets a list of edge ids between this vertex and another vertex.
     *
     * @param otherVertex    The other vertex to find edges on.
     * @param direction      The direction of the edge.
     * @param labels         The labels to search for.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge ids.
     */
    Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets count of edges on this vertex.
     *
     * @param direction      The direction of the edges to get a count on.
     * @param authorizations The authorizations used to find the edges.
     * @return The count of edges.
     */
    int getEdgeCount(Direction direction, Authorizations authorizations);

    /**
     * Gets a list of edge labels.
     *
     * @param direction      The direction of the edge.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of edge labels.
     */
    Iterable<String> getEdgeLabels(Direction direction, Authorizations authorizations);

    /**
     * Gets edge summary information
     *
     * @param authorizations The authorizations used to get the edges summary
     * @return The edges summary
     */
    EdgesSummary getEdgesSummary(Authorizations authorizations);

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction      The direction of the edge.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    Iterable<EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations);

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction      The direction of the edge.
     * @param label          The label of edges to traverse to find the edge infos.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    Iterable<EdgeInfo> getEdgeInfos(Direction direction, String label, Authorizations authorizations);

    /**
     * Get a list of EdgeInfo.
     *
     * @param direction      The direction of the edge.
     * @param labels         The labels of edges to traverse to find the edge infos.
     * @param authorizations The authorizations used to find the edges.
     * @return An Iterable of EdgeInfo.
     */
    Iterable<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex.
     *
     * @param direction      The direction relative to this vertex.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex.
     *
     * @param direction      The direction relative to this vertex.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have the given label.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Similar to getEdges but gets the vertices on the other side of the edges attached to this vertex that have any of the given labels.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertices.
     */
    Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param label          The label of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param labels         The labels of edges to traverse to find the vertices.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets vertex ids of connected vertices.
     *
     * @param direction      The direction relative to this vertex.
     * @param authorizations The authorizations used to find the vertices.
     * @return An Iterable of vertex ids.
     */
    Iterable<String> getVertexIds(Direction direction, Authorizations authorizations);

    /**
     * Creates a query to query the edges and vertices attached to this vertex.
     *
     * @param authorizations The authorizations used to find the edges and vertices.
     * @return The query builder.
     */
    VertexQuery query(Authorizations authorizations);

    /**
     * Creates a query to query the edges and vertices attached to this vertex.
     *
     * @param queryString    The string to search for.
     * @param authorizations The authorizations used to find the edges and vertices.
     * @return The query builder.
     */
    VertexQuery query(String queryString, Authorizations authorizations);

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * Graph#prepareVertex(Visibility, Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    ExistingVertexMutation prepareMutation();

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param endTime        Include all changes made up until the point in time.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs with the given label attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param label          The edge label to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, Authorizations authorizations);

    /**
     * Gets all edge/vertex pairs with any of the given labels attached to this vertex.
     *
     * @param direction      The side of the edge that this vertex is attached to.
     * @param labels         An array of edge labels to search for.
     * @param fetchHints     Hint on what should be fetched from the datastore.
     * @param authorizations The authorizations used to find the edge/vertex pairs.
     * @return An Iterable of edge/vertex pairs.
     */
    Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations);

    @Override
    default ElementType getElementType() {
        return ElementType.VERTEX;
    }

    String getConceptType();
}
