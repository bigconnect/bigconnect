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

import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.LookAheadIterable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class EdgeVertexPair implements GraphElementContainer {
    private final Edge edge;
    private final Vertex vertex;

    public EdgeVertexPair(Edge edge, Vertex vertex) {
        this.edge = edge;
        this.vertex = vertex;
    }

    public Edge getEdge() {
        return edge;
    }

    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public String toString() {
        return "EdgeVertexPair{" +
                "edge=" + edge +
                ", vertex=" + vertex +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EdgeVertexPair that = (EdgeVertexPair) o;

        if (!edge.equals(that.edge)) {
            return false;
        }
        if (!vertex.equals(that.vertex)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = edge.hashCode();
        result = 31 * result + vertex.hashCode();
        return result;
    }

    public static Iterable<EdgeVertexPair> getEdgeVertexPairs(
            Graph graph,
            String sourceVertexId,
            Iterable<EdgeInfo> edgeInfos,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        Set<String> edgeIdsToFetch = new HashSet<>();
        Set<String> vertexIdsToFetch = new HashSet<>();
        for (EdgeInfo edgeInfo : edgeInfos) {
            edgeIdsToFetch.add(edgeInfo.getEdgeId());
            vertexIdsToFetch.add(edgeInfo.getVertexId());
        }
        final Map<String, Vertex> vertices = IterableUtils.toMapById(graph.getVertices(vertexIdsToFetch, fetchHints, endTime, authorizations));
        return new LookAheadIterable<Edge, EdgeVertexPair>() {
            @Override
            protected boolean isIncluded(Edge edge, EdgeVertexPair edgeVertexPair) {
                String otherVertexId = edge.getOtherVertexId(sourceVertexId);
                return vertices.get(otherVertexId) != null;
            }

            @Override
            protected EdgeVertexPair convert(Edge edge) {
                String otherVertexId = edge.getOtherVertexId(sourceVertexId);
                Vertex otherVertex = vertices.get(otherVertexId);
                return new EdgeVertexPair(edge, otherVertex);
            }

            @Override
            protected Iterator<Edge> createIterator() {
                return graph.getEdges(edgeIdsToFetch, fetchHints, endTime, authorizations).iterator();
            }
        };
    }
}
