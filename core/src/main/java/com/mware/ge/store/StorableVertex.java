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
package com.mware.ge.store;

import com.google.common.collect.ImmutableSet;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingVertexMutation;
import com.mware.ge.mutation.PropertyDeleteMutation;
import com.mware.ge.mutation.PropertySoftDeleteMutation;
import com.mware.ge.query.VertexQuery;
import com.mware.ge.store.util.GetVertexIdsIterable;
import com.mware.ge.store.util.SoftDeleteEdgeInfo;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.FilterIterable;
import com.mware.ge.util.JoinIterable;
import com.mware.ge.util.LookAheadIterable;

import java.util.*;

public class StorableVertex extends StorableElement implements Vertex {
    public static final String CF_SIGNAL = "V";
    public static final String CF_OUT_EDGE = "EOUT";
    public static final String CF_IN_EDGE = "EIN";
    public static final String CF_OUT_EDGE_SOFT_DELETE = "EOUTD";
    public static final String CF_IN_EDGE_SOFT_DELETE = "EIND";
    public static final String CF_OUT_EDGE_HIDDEN = "EOUTH";
    public static final String CF_IN_EDGE_HIDDEN = "EINH";

    private final Edges inEdges;
    private final Edges outEdges;

    private List<SoftDeleteEdgeInfo> outSoftDeletes = new ArrayList<>();
    private List<SoftDeleteEdgeInfo> inSoftDeletes = new ArrayList<>();
    private Set<String> hiddenEdges = new HashSet<>();
    private String conceptType;
    private String newConceptType;

    public StorableVertex(
            StorableGraph graph,
            String vertexId,
            String conceptType,
            String newConceptType,
            Visibility vertexVisibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            ImmutableSet<String> extendedDataTableNames,
            long timestamp,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        this(graph, vertexId, conceptType, newConceptType, vertexVisibility, properties, propertyDeleteMutations, propertySoftDeleteMutations,
                hiddenVisibilities, extendedDataTableNames, new EdgesWithEdgeInfo(), new EdgesWithEdgeInfo(), timestamp,
                fetchHints, authorizations);
    }

    public StorableVertex(
            StorableGraph graph,
            String vertexId,
            String conceptType,
            String newConceptType,
            Visibility vertexVisibility,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeleteMutations,
            Iterable<PropertySoftDeleteMutation> propertySoftDeleteMutations,
            Iterable<Visibility> hiddenVisibilities,
            ImmutableSet<String> extendedDataTableNames,
            Edges inEdges,
            Edges outEdges,
            long timestamp,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        super(
                graph,
                vertexId,
                vertexVisibility,
                properties,
                propertyDeleteMutations,
                propertySoftDeleteMutations,
                hiddenVisibilities,
                extendedDataTableNames,
                timestamp,
                fetchHints,
                authorizations
        );

        this.inEdges = inEdges;
        this.outEdges = outEdges;
        this.conceptType = conceptType;
        this.newConceptType = newConceptType;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, FetchHints fetchHints, Long endTime, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getGraph().getEdges(getEdgeIds(direction, authorizations), fetchHints, endTime, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getEdgeIdsWithOtherVertexId(null, direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getGraph().getEdges(getEdgeIds(direction, labelToArrayOrNull(label), authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Direction direction, String label, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getEdgeIdsWithOtherVertexId(null, direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, final String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(final Direction direction, final String[] labels, final Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getEdgeIdsWithOtherVertexId(null, direction, labels, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction);
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, null, authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String label, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labelToArrayOrNull(label), authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(Vertex otherVertex, Direction direction, String label, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, label);
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labelToArrayOrNull(label), authorizations);
    }

    @Override
    public Iterable<Edge> getEdges(final Vertex otherVertex, Direction direction, String[] labels, FetchHints fetchHints, Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getGraph().getEdges(getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getEdgeIds(final Vertex otherVertex, final Direction direction, final String[] labels, final Authorizations authorizations) {
        getFetchHints().validateHasEdgeFetchHints(direction, labels);
        return getEdgeIdsWithOtherVertexId(otherVertex.getId(), direction, labels, authorizations);
    }

    @Override
    public EdgesSummary getEdgesSummary(Authorizations authorizations) {
        Map<String, Integer> outEdgeCountsByLabels = new HashMap<>();
        Map<String, Integer> inEdgeCountsByLabels = new HashMap<>();

        if (inEdges instanceof EdgesWithCount) {
            EdgesWithCount edgesWithCount = (EdgesWithCount) this.inEdges;
            inEdgeCountsByLabels.putAll(edgesWithCount.getEdgeCountsByLabelName());
        } else {
            for (Map.Entry<String, StorableEdgeInfo> entry : getEdgeInfos(Direction.IN)) {
                String label = entry.getValue().getLabel();
                Integer c = inEdgeCountsByLabels.getOrDefault(label, 0);
                inEdgeCountsByLabels.put(label, c + 1);
            }
        }

        if (outEdges instanceof EdgesWithCount) {
            EdgesWithCount edgesWithCount = (EdgesWithCount) this.outEdges;
            outEdgeCountsByLabels.putAll(edgesWithCount.getEdgeCountsByLabelName());
        } else {
            for (Map.Entry<String, StorableEdgeInfo> entry : getEdgeInfos(Direction.OUT)) {
                String label = entry.getValue().getLabel();
                Integer c = outEdgeCountsByLabels.getOrDefault(label, 0);
                outEdgeCountsByLabels.put(label, c + 1);
            }
        }

        return new EdgesSummary(outEdgeCountsByLabels, inEdgeCountsByLabels);
    }

    @SuppressWarnings("unused")
    public Iterable<String> getEdgeIdsWithOtherVertexId(
            String otherVertexId,
            Direction direction,
            String[] labels,
            Authorizations authorizations
    ) {
        if (!getFetchHints().isIncludeEdgeRefs()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeRefs");
        }

        if (!getFetchHints().isIncludeEdgeIds()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeIds");
        }

        return new LookAheadIterable<Map.Entry<String, StorableEdgeInfo>, String>() {
            @Override
            protected boolean isIncluded(Map.Entry<String, StorableEdgeInfo> edgeInfo, String edgeId) {
                if (otherVertexId != null) {
                    if (!otherVertexId.equals(edgeInfo.getValue().getVertexId())) {
                        return false;
                    }
                }
                if (labels == null || labels.length == 0) {
                    return true;
                }

                for (String label : labels) {
                    if (label.equals(edgeInfo.getValue().getLabel())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected String convert(Map.Entry<String, StorableEdgeInfo> edgeInfo) {
                return edgeInfo.getKey();
            }

            @Override
            protected Iterator<Map.Entry<String, StorableEdgeInfo>> createIterator() {
                return getEdgeInfos(direction).iterator();
            }
        };
    }


    private Iterable<Map.Entry<String, StorableEdgeInfo>> getEdgeInfos(Direction direction) {
        if (!getFetchHints().isIncludeEdgeRefs()) {
            throw new GeException("getEdgeInfos called without including any edge infos");
        }
        switch (direction) {
            case IN:
                if (!getFetchHints().isIncludeInEdgeRefs() && !getFetchHints().hasEdgeLabelsOfEdgeRefsToInclude()) {
                    return null;
                }
                if (this.inEdges instanceof EdgesWithEdgeInfo) {
                    return ((EdgesWithEdgeInfo) this.inEdges).getEntries();
                }
                throw new GeException("Cannot get edge info");
            case OUT:
                if (!getFetchHints().isIncludeOutEdgeRefs() && !getFetchHints().hasEdgeLabelsOfEdgeRefsToInclude()) {
                    return null;
                }
                if (this.outEdges instanceof EdgesWithEdgeInfo) {
                    return ((EdgesWithEdgeInfo) this.outEdges).getEntries();
                }
                throw new GeException("Cannot get edge info");
            case BOTH:
                return new JoinIterable<>(getEdgeInfos(Direction.IN), getEdgeInfos(Direction.OUT));
            default:
                throw new GeException("Unexpected direction: " + direction);
        }
    }

    @Override
    public Iterable<com.mware.ge.EdgeInfo> getEdgeInfos(Direction direction, String[] labels, Authorizations authorizations) {
        if (!getFetchHints().isIncludeEdgeRefs()) {
            throw new GeException("getEdgeInfos called without including any edge infos");
        }

        switch (direction) {
            case IN:
                return filterEdgeInfosByLabel(storableEdgeInfosToEdgeInfos(getEdgeInfos(direction), Direction.IN), labels);
            case OUT:
                return filterEdgeInfosByLabel(storableEdgeInfosToEdgeInfos(getEdgeInfos(direction), Direction.OUT), labels);
            case BOTH:
                return new JoinIterable<>(getEdgeInfos(Direction.IN, labels, authorizations), getEdgeInfos(Direction.OUT, labels, authorizations));
            default:
                throw new GeException("Unexpected direction: " + direction);
        }
    }

    private Iterable<com.mware.ge.EdgeInfo> storableEdgeInfosToEdgeInfos(Iterable<Map.Entry<String, StorableEdgeInfo>> edgeInfos, Direction direction) {
        return new ConvertingIterable<Map.Entry<String, StorableEdgeInfo>, com.mware.ge.EdgeInfo>(edgeInfos) {
            @Override
            protected com.mware.ge.EdgeInfo convert(Map.Entry<String, StorableEdgeInfo> o) {
                final String edgeId = o.getKey() == null ? null : o.getKey();
                final StorableEdgeInfo edgeInfo = o.getValue();
                return new EdgeInfo() {
                    @Override
                    public String getEdgeId() {
                        if (!getFetchHints().isIncludeEdgeIds()) {
                            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeIds");
                        }
                        return edgeId;
                    }

                    @Override
                    public String getLabel() {
                        return edgeInfo.getLabel();
                    }

                    @Override
                    public String getVertexId() {
                        if (!getFetchHints().isIncludeEdgeVertexIds()) {
                            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeVertexIds");
                        }
                        return edgeInfo.getVertexId();
                    }

                    @Override
                    public Direction getDirection() {
                        return direction;
                    }
                };
            }
        };
    }

    protected Iterable<EdgeInfo> filterEdgeInfosByLabel(Iterable<com.mware.ge.EdgeInfo> edgeInfos, String[] labels) {
        if (labels != null) {
            return new FilterIterable<EdgeInfo>(edgeInfos) {
                @Override
                protected boolean isIncluded(EdgeInfo o) {
                    for (String label : labels) {
                        if (o.getLabel().equals(label)) {
                            return true;
                        }
                    }
                    return false;
                }
            };
        }
        return edgeInfos;
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, FetchHints fetchHints, final Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String[] labels, FetchHints fetchHints, final Authorizations authorizations) {
        return getGraph().getVertices(getVertexIds(direction, labels, authorizations), fetchHints, authorizations);
    }

    @Override
    public Iterable<String> getVertexIds(Direction direction, String[] labels, Authorizations authorizations) {
        if (!getFetchHints().isIncludeEdgeVertexIds()) {
            throw new GeMissingFetchHintException(getFetchHints(), "includeEdgeVertexIds");
        }
        switch (direction) {
            case BOTH:
                Iterable<String> inVertexIds = getVertexIds(Direction.IN, labels, authorizations);
                Iterable<String> outVertexIds = getVertexIds(Direction.OUT, labels, authorizations);
                return new JoinIterable<>(inVertexIds, outVertexIds);
            case IN:
                if (this.inEdges instanceof EdgesWithEdgeInfo) {
                    return new GetVertexIdsIterable(((EdgesWithEdgeInfo) this.inEdges).getEdgeInfos(), labels);
                }
                throw new GeException("Cannot get vertex ids");
            case OUT:
                if (this.outEdges instanceof EdgesWithEdgeInfo) {
                    return new GetVertexIdsIterable(((EdgesWithEdgeInfo) this.outEdges).getEdgeInfos(), labels);
                }
                throw new GeException("Cannot get vertex ids");
            default:
                throw new GeException("Unexpected direction: " + direction);
        }
    }

    @Override
    public VertexQuery query(String queryString, Authorizations authorizations) {
        return getGraph().getSearchIndex().queryVertex(getGraph(), this, queryString, authorizations);
    }

    protected void addOutEdge(Edge edge) {
        if (this.outEdges instanceof EdgesWithEdgeInfo) {
            ((EdgesWithEdgeInfo) this.outEdges).add(edge.getId(), new StorableEdgeInfo(edge.getLabel(), edge.getVertexId(Direction.IN)));
        } else {
            throw new GeException("Cannot add edge");
        }
    }

    protected void removeOutEdge(Edge edge) {
        if (this.outEdges instanceof EdgesWithEdgeInfo) {
            ((EdgesWithEdgeInfo) this.outEdges).remove(edge.getId());
        } else {
            throw new GeException("Cannot remove out edge");
        }
    }

    protected void addInEdge(Edge edge) {
        if (this.inEdges instanceof EdgesWithEdgeInfo) {
            ((EdgesWithEdgeInfo) this.inEdges).add(edge.getId(), new StorableEdgeInfo(edge.getLabel(), edge.getVertexId(Direction.OUT)));
        } else {
            throw new GeException("Cannot add edge");
        }
    }

    protected void removeInEdge(Edge edge) {
        if (this.inEdges instanceof EdgesWithEdgeInfo) {
            ((EdgesWithEdgeInfo) this.inEdges).remove(edge.getId());
        } else {
            throw new GeException("Cannot remove in edge");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ExistingVertexMutation prepareMutation() {
        return new ExistingVertexMutation(this) {
            @Override
            public Vertex save(Authorizations authorizations) {
                saveExistingElementMutation(this, authorizations);
                return getElement();
            }
        };
    }

    private static String[] labelToArrayOrNull(String label) {
        return label == null ? null : new String[]{label};
    }

    @Override
    public String getConceptType() {
        return conceptType;
    }

    public void setConceptType(String newConceptType) {
        this.conceptType = newConceptType;
    }

    public String getNewConceptType() {
        return newConceptType;
    }
}
