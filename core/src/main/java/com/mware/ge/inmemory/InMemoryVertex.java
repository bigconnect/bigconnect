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

import com.mware.ge.*;
import com.mware.ge.inmemory.mutations.AlterConceptTypeMutation;
import com.mware.ge.inmemory.util.EdgeToEdgeIdIterable;
import com.mware.ge.mutation.ExistingVertexMutation;
import com.mware.ge.query.VertexQuery;
import com.mware.ge.search.IndexHint;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.FilterIterable;

import java.util.HashMap;
import java.util.Map;

public class InMemoryVertex extends InMemoryElement<InMemoryVertex> implements Vertex {

    public InMemoryVertex(
            InMemoryGraph graph,
            String id,
            InMemoryTableVertex inMemoryTableElement,
            FetchHints fetchHints,
            Long endTime,
            Authorizations authorizations
    ) {
        super(
                graph,
                id,
                inMemoryTableElement,
                fetchHints,
                endTime,
                authorizations
        );
    }

    @Override
    public String getConceptType() {
        return getInMemoryTableElement().findLastMutation(AlterConceptTypeMutation.class).getNewConceptType();
    }

    @Override
    public Iterable<EdgeInfo> getEdgeInfos(Direction direction, final String[] labels, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        Iterable<EdgeInfo> results = internalGetEdgeInfo(direction, authorizations);
        results = new FilterIterable<EdgeInfo>(results) {
            @Override
            protected boolean isIncluded(EdgeInfo o) {
                if (!getFetchHints().isIncludeEdgeRefLabel(o.getLabel())) {
                    return false;
                }
                if (labels == null) {
                    return true;
                } else {
                    for (String label : labels) {
                        if (o.getLabel().equals(label)) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        };
        return results;
    }

    private Iterable<EdgeInfo> internalGetEdgeInfo(Direction direction, Authorizations authorizations) {
        return new ConvertingIterable<Edge, EdgeInfo>(internalGetEdges(direction, getFetchHints(), null, authorizations)) {
            @Override
            protected EdgeInfo convert(Edge edge) {
                return new EdgeInfo() {
                    @Override
                    public String getEdgeId() {
                        if (!getFetchHints().isIncludeEdgeIds()) {
                            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeIds");
                        }
                        return edge.getId();
                    }

                    @Override
                    public String getLabel() {
                        return edge.getLabel();
                    }

                    @Override
                    public String getVertexId() {
                        if (!getFetchHints().isIncludeEdgeVertexIds()) {
                            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeVertexIds");
                        }
                        return edge.getOtherVertexId(InMemoryVertex.this.getId());
                    }

                    @Override
                    public Direction getDirection() {
                        return edge.getVertexId(Direction.OUT).equals(edge.getOtherVertexId(InMemoryVertex.this.getId()))
                                ? Direction.IN
                                : Direction.OUT;
                    }
                };
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        if (!getFetchHints().isIncludeEdgeIds()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeIds");
        }
        return internalGetEdges(direction, fetchHints, endTime, authorizations);
    }

    private Iterable<Edge> internalGetEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        return new FilterIterable<Edge>(getGraph().getEdgesFromVertex(getId(), fetchHints, endTime, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                switch (direction) {
                    case IN:
                        return edge.getVertexId(Direction.IN).equals(getId());
                    case OUT:
                        return edge.getVertexId(Direction.OUT).equals(getId());
                    default:
                        return true;
                }
            }
        };
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        if (!getFetchHints().isIncludeEdgeIds()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeIds");
        }
        return new EdgeToEdgeIdIterable(getEdges(direction, getFetchHints(), authorizations));
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        if (!getFetchHints().isIncludeEdgeIds()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeIds");
        }
        return new EdgeToEdgeIdIterable(getEdges(direction, label, authorizations));
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String[] labels, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(direction, labels, authorizations));
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(otherVertex, direction, authorizations));
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(otherVertex, direction, label, authorizations));
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String[] labels, Authorizations authorizations) {
        return new EdgeToEdgeIdIterable(getEdges(otherVertex, direction, labels, authorizations));
    }

    @Override
    public EdgesSummary getEdgesSummary(Authorizations authorizations) {
        Map<String, Integer> outEdgeCountsByLabels = new HashMap<>();
        Map<String, Integer> inEdgeCountsByLabels = new HashMap<>();
        for (EdgeInfo entry : internalGetEdgeInfo(Direction.IN, authorizations)) {
            String label = entry.getLabel();
            Integer c = inEdgeCountsByLabels.getOrDefault(label, 0);
            inEdgeCountsByLabels.put(label, c + 1);
        }
        for (EdgeInfo entry : internalGetEdgeInfo(Direction.OUT, authorizations)) {
            String label = entry.getLabel();
            Integer c = outEdgeCountsByLabels.getOrDefault(label, 0);
            outEdgeCountsByLabels.put(label, c + 1);
        }
        return new EdgesSummary(outEdgeCountsByLabels, inEdgeCountsByLabels);
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, queryString, authorizations);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExistingVertexMutation prepareMutation() {
        return new ExistingVertexMutation(this) {
            @Override
            public Vertex save(Authorizations authorizations) {
                IndexHint indexHint = getIndexHint();
                saveExistingElementMutation(this, indexHint, authorizations);
                Vertex vertex = getElement();
                if (indexHint != IndexHint.DO_NOT_INDEX) {
                    getGraph().updateElementAndExtendedDataInSearchIndex(vertex, this, authorizations);
                }
                return vertex;
            }
        };
    }
}
