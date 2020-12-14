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
package com.mware.ge.query;

import com.mware.ge.*;
import com.mware.ge.query.aggregations.Aggregation;
import com.mware.ge.util.FilterIterable;
import com.mware.ge.util.JoinIterable;

import java.util.List;

public class DefaultVertexQuery extends VertexQueryBase implements VertexQuery {
    public DefaultVertexQuery(Graph graph, Vertex sourceVertex, String queryString, Authorizations authorizations) {
        super(graph, sourceVertex, queryString, authorizations);
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(FetchHints fetchHints) {
        Iterable<Vertex> vertices = allVertices(fetchHints);
        return new DefaultGraphQueryIterableWithAggregations<>(getParameters(), vertices, true, true, true, getAggregations());
    }

    private Iterable<Vertex> allVertices(FetchHints fetchHints) {
        List<String> edgeLabels = getParameters().getEdgeLabels();
        String[] edgeLabelsArray = edgeLabels == null || edgeLabels.size() == 0
                ? null
                : edgeLabels.toArray(new String[edgeLabels.size()]);
        Iterable<Vertex> results = getSourceVertex().getVertices(
                getDirection(),
                edgeLabelsArray,
                fetchHints,
                getParameters().getAuthorizations()
        );
        if (getOtherVertexId() != null) {
            results = new FilterIterable<Vertex>(results) {
                @Override
                protected boolean isIncluded(Vertex otherVertex) {
                    return otherVertex.getId().equals(getOtherVertexId());
                }
            };
        }
        if (getParameters().getIds() != null) {
            results = new FilterIterable<Vertex>(results) {
                @Override
                protected boolean isIncluded(Vertex otherVertex) {
                    return getParameters().getIds().contains(otherVertex.getId());
                }
            };
        }
        return results;
    }

    @Override
    public QueryResultsIterable<Edge> edges(FetchHints fetchHints) {
        Iterable<Edge> edges = allEdges(fetchHints);
        return new DefaultGraphQueryIterableWithAggregations<>(getParameters(), edges, true, true, true, getAggregations());
    }

    private Iterable<Edge> allEdges(FetchHints fetchHints) {
        Iterable<Edge> results = getSourceVertex().getEdges(getDirection(), fetchHints, getParameters().getAuthorizations());
        if (getOtherVertexId() != null) {
            results = new FilterIterable<Edge>(results) {
                @Override
                protected boolean isIncluded(Edge edge) {
                    return edge.getOtherVertexId(getSourceVertex().getId()).equals(getOtherVertexId());
                }
            };
        }
        return results;
    }

    @Override
    protected QueryResultsIterable<? extends GeObject> extendedData(FetchHints extendedDataFetchHints) {
        FetchHints fetchHints = FetchHints.builder()
                .setIncludeExtendedDataTableNames(true)
                .build();
        return extendedData(extendedDataFetchHints, new JoinIterable<>(
                allVertices(fetchHints),
                allEdges(fetchHints)
        ));
    }

    @Override
    public boolean isAggregationSupported(Aggregation aggregation) {
        if (DefaultGraphQueryIterableWithAggregations.isAggregationSupported(aggregation)) {
            return true;
        }
        return super.isAggregationSupported(aggregation);
    }
}
