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
package com.mware.ge.elasticsearch5;

import com.mware.ge.*;
import com.mware.ge.mutation.ExistingVertexMutation;
import com.mware.ge.query.VertexQuery;
import com.mware.ge.query.builder.GeQueryBuilder;

public class ElasticsearchVertex extends ElasticsearchElement implements Vertex {
    private String className = ElasticsearchElement.class.getSimpleName();
    private String conceptType;

    public ElasticsearchVertex(
            Graph graph,
            String id,
            String conceptType,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        super(graph, id, fetchHints, authorizations);
        this.conceptType = conceptType;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<Edge> getEdges(Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdges is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeIds is not supported on " + className);
    }

    @Override
    public int getEdgeCount(Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeCount is not supported on " + className);
    }

    @Override
    public Iterable<String> getEdgeLabels(Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeLabels is not supported on " + className);
    }

    @Override
    public EdgesSummary getEdgesSummary(Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgesSummary is not supported on " + className);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeInfos is not supported on " + className);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, String label, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeInfos is not supported on " + className);
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeInfos is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getProperties is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, Authorizations authorizations) {
        throw new GeNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, Authorizations authorizations) {
        throw new GeNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getVertices is not supported on " + className);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String label, Authorizations authorizations) {
        throw new GeNotSupportedException("getVertexIds is not supported on " + className);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        throw new GeNotSupportedException("getVertexIds is not supported on " + className);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getVertexIds is not supported on " + className);
    }

    @Override
    public VertexQuery query(GeQueryBuilder queryBuilder, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, queryBuilder, authorizations);
    }

    @Override
    public ExistingVertexMutation prepareMutation() {
        throw new GeNotSupportedException("prepareMutation is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public Iterable<EdgeVertexPair> getEdgeVertexPairs(Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        throw new GeNotSupportedException("getEdgeVertexPairs is not supported on " + className);
    }

    @Override
    public String getConceptType() {
        return conceptType;
    }
}
