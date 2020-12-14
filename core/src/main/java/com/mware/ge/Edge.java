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

import com.mware.ge.mutation.ExistingEdgeMutation;
import com.mware.ge.util.IterableUtils;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public interface Edge extends Element, EdgeElementLocation {
    /**
     * Meta property name used for queries, sorting, and aggregations
     */
    String LABEL_PROPERTY_NAME = "__edgeLabel";

    /**
     * Meta property name used for queries, sorting, and aggregations
     */
    String OUT_VERTEX_ID_PROPERTY_NAME = "__outVertexId";

    /**
     * Meta property name used for queries, sorting, and aggregations
     */
    String OUT_VERTEX_TYPE_PROPERTY_NAME = "__outVertexType";

    /**
     * Meta property name used for queries, sorting, and aggregations
     */
    String IN_VERTEX_ID_PROPERTY_NAME = "__inVertexId";

    /**
     * Meta property name used for queries, sorting, and aggregations
     */
    String IN_VERTEX_TYPE_PROPERTY_NAME = "__inVertexType";

    /**
     * Meta property name used for queries. The property represents either the in or the out vertex id.
     */
    String IN_OR_OUT_VERTEX_ID_PROPERTY_NAME = "__inOrOutVertexId";

    /**
     * The edge label.
     */
    String getLabel();

    /**
     * Get the attach vertex id on either side of the edge.
     *
     * @param direction The side of the edge to get the vertex id from (IN or OUT).
     * @return The id of the vertex.
     */
    String getVertexId(Direction direction);

    /**
     * Get the attach vertex on either side of the edge.
     *
     * @param direction The side of the edge to get the vertex from (IN or OUT).
     * @return The vertex.
     */
    default Vertex getVertex(Direction direction, Authorizations authorizations) {
        return getVertex(direction, getGraph().getDefaultFetchHints(), authorizations);
    }

    default String getVertexType(Direction direction, Authorizations authorizations) {
        Vertex v = getVertex(direction, FetchHints.NONE, authorizations);
        if(v != null)
            return v.getConceptType();
        else
            return null;
    }

    /**
     * Get the attach vertex on either side of the edge.
     *
     * @param direction  The side of the edge to get the vertex from (IN or OUT).
     * @param fetchHints Hint on what should be fetched from the datastore.
     * @return The vertex.
     */
    default Vertex getVertex(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        String vertexId = getVertexId(direction);
        return getGraph().getVertex(vertexId, fetchHints, authorizations);
    }

    /**
     * Given a vertexId that represents one side of a relationship, get me the id of the other side.
     */
    String getOtherVertexId(String myVertexId);

    /**
     * Given a vertexId that represents one side of a relationship, get me the vertex of the other side.
     */
    default Vertex getOtherVertex(String myVertexId, Authorizations authorizations) {
        return getOtherVertex(myVertexId, getGraph().getDefaultFetchHints(), authorizations);
    }

    /**
     * Given a vertexId that represents one side of a relationship, get me the vertex of the other side.
     */
    default Vertex getOtherVertex(String myVertexId, FetchHints fetchHints, Authorizations authorizations) {
        String vertexId = getOtherVertexId(myVertexId);
        return getGraph().getVertex(vertexId, fetchHints, authorizations);
    }

    /**
     * Gets both in and out vertices of this edge.
     */
    default EdgeVertices getVertices(Authorizations authorizations) {
        return getVertices(getGraph().getDefaultFetchHints(), authorizations);
    }

    /**
     * Gets both in and out vertices of this edge.
     */
    default EdgeVertices getVertices(FetchHints fetchHints, Authorizations authorizations) {
        List<String> ids = new ArrayList<>();
        ids.add(getVertexId(Direction.OUT));
        ids.add(getVertexId(Direction.IN));
        Map<String, Vertex> vertices = IterableUtils.toMapById(getGraph().getVertices(ids, fetchHints, authorizations));
        return new EdgeVertices(
                vertices.get(getVertexId(Direction.OUT)),
                vertices.get(getVertexId(Direction.IN))
        );
    }

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * Graph#prepareEdge(Visibility, Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    ExistingEdgeMutation prepareMutation();

    @Override
    default ElementType getElementType() {
        return ElementType.EDGE;
    }
}

