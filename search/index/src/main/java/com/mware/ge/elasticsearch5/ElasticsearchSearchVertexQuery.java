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
import com.mware.ge.collection.Iterables;
import com.mware.ge.query.builder.EdgeLabelQueryBuilder;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.query.builder.GeQueryBuilders;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import com.mware.ge.query.VertexQuery;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import static com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME;
import static com.mware.ge.util.StreamUtils.stream;

public class ElasticsearchSearchVertexQuery extends ElasticsearchSearchQueryBase implements VertexQuery {
    private final Vertex sourceVertex;
    private Direction direction = Direction.BOTH;
    private String otherVertexId;

    public ElasticsearchSearchVertexQuery(
            Client client,
            Graph graph,
            Vertex sourceVertex,
            GeQueryBuilder queryBuilder,
            Options options,
            Authorizations authorizations
    ) {
        super(client, graph, queryBuilder, options, authorizations);
        this.sourceVertex = sourceVertex;
    }

    @Override
    protected List<QueryBuilder> getInternalFilters(EnumSet<ElasticsearchDocumentType> elementTypes, FetchHints fetchHints) {
        List<QueryBuilder> filters = super.getInternalFilters(elementTypes, fetchHints);

        List<QueryBuilder> relatedFilters = new ArrayList<>();

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX)
                || elementTypes.contains(ElasticsearchDocumentType.VERTEX_EXTENDED_DATA)) {
            relatedFilters.add(getVertexFilter(elementTypes));
        }

        if (elementTypes.contains(ElasticsearchDocumentType.EDGE)
                || elementTypes.contains(ElasticsearchDocumentType.EDGE_EXTENDED_DATA)) {
            relatedFilters.add(getEdgeFilter());
        }

        filters.add(orFilters(relatedFilters));

        return filters;
    }

    private QueryBuilder getEdgeFilter() {
        switch (direction) {
            case BOTH:
                QueryBuilder inVertexIdFilter = getDirectionInEdgeFilter();
                QueryBuilder outVertexIdFilter = getDirectionOutEdgeFilter();
                return QueryBuilders.boolQuery()
                        .should(inVertexIdFilter)
                        .should(outVertexIdFilter)
                        .minimumShouldMatch(1);
            case OUT:
                return getDirectionOutEdgeFilter();
            case IN:
                return getDirectionInEdgeFilter();
            default:
                throw new GeException("unexpected direction: " + direction);
        }
    }

    private QueryBuilder getDirectionInEdgeFilter() {
        QueryBuilder outVertexIdFilter = QueryBuilders.termQuery(Elasticsearch5SearchIndex.IN_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        if (otherVertexId != null) {
            QueryBuilder inVertexIdFilter = QueryBuilders.termQuery(Elasticsearch5SearchIndex.OUT_VERTEX_ID_FIELD_NAME, otherVertexId);
            return QueryBuilders.boolQuery()
                    .must(outVertexIdFilter)
                    .must(inVertexIdFilter);
        }
        return outVertexIdFilter;
    }

    private QueryBuilder getDirectionOutEdgeFilter() {
        QueryBuilder outVertexIdFilter = QueryBuilders.termQuery(Elasticsearch5SearchIndex.OUT_VERTEX_ID_FIELD_NAME, sourceVertex.getId());
        if (otherVertexId != null) {
            QueryBuilder inVertexIdFilter = QueryBuilders.termQuery(Elasticsearch5SearchIndex.IN_VERTEX_ID_FIELD_NAME, otherVertexId);
            return QueryBuilders.boolQuery()
                    .must(outVertexIdFilter)
                    .must(inVertexIdFilter);
        }
        return outVertexIdFilter;
    }

    private QueryBuilder getVertexFilter(EnumSet<ElasticsearchDocumentType> elementTypes) {
        Iterable<Vertex> results = sourceVertex.getVertices(direction, FetchHints.PROPERTIES_AND_EDGE_REFS, getAuthorizations());
        results = Iterables.filter(results, v -> getBuilder().matches(v, getAuthorizations()));
        if (otherVertexId != null) {
            results = Iterables.filter(results, v -> v.getId().equals(otherVertexId));
        }

        List<QueryBuilder> filters = new ArrayList<>();
        String[] ids = Iterables.asArray(String.class, Iterables.map(results, ElementId::getId));

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX)) {
            filters.add(QueryBuilders.termsQuery(ELEMENT_ID_FIELD_NAME, ids));
        }

        if (elementTypes.contains(ElasticsearchDocumentType.VERTEX_EXTENDED_DATA)) {
            for (String vertexId : ids) {
                filters.add(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME, ElasticsearchDocumentType.VERTEX_EXTENDED_DATA.getKey()))
                                .must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME, vertexId)));
            }
        }

        // remove edgelabelquerybuilder from query
        if (getBuilder() instanceof EdgeLabelQueryBuilder) {
            setBuilder(GeQueryBuilders.searchAll());
        } else if (getBuilder() instanceof com.mware.ge.query.builder.BoolQueryBuilder) {
            removeEdgeLabelQuery((com.mware.ge.query.builder.BoolQueryBuilder) getBuilder());
        }

        return orFilters(filters);
    }

    private void removeEdgeLabelQuery(com.mware.ge.query.builder.BoolQueryBuilder bqb) {
        for (int i = 0; i < bqb.getAndClauses().size(); i++) {
            GeQueryBuilder qb = bqb.getAndClauses().get(i);
            if (qb instanceof EdgeLabelQueryBuilder)
                bqb.getAndClauses().remove(i);
            else if (qb instanceof com.mware.ge.query.builder.BoolQueryBuilder)
                removeEdgeLabelQuery((com.mware.ge.query.builder.BoolQueryBuilder) qb);
        }
        for (int i = 0; i < bqb.getNotClauses().size(); i++) {
            GeQueryBuilder qb = bqb.getNotClauses().get(i);
            if (qb instanceof EdgeLabelQueryBuilder)
                bqb.getNotClauses().remove(i);
            else if (qb instanceof com.mware.ge.query.builder.BoolQueryBuilder)
                removeEdgeLabelQuery((com.mware.ge.query.builder.BoolQueryBuilder) qb);
        }
        for (int i = 0; i < bqb.getOrClauses().size(); i++) {
            GeQueryBuilder qb = bqb.getOrClauses().get(i);
            if (qb instanceof EdgeLabelQueryBuilder)
                bqb.getOrClauses().remove(i);
            else if (qb instanceof com.mware.ge.query.builder.BoolQueryBuilder)
                removeEdgeLabelQuery((com.mware.ge.query.builder.BoolQueryBuilder) qb);
        }
    }

    private QueryBuilder orFilters(List<QueryBuilder> filters) {
        if (filters.size() == 1) {
            return filters.get(0);
        } else {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            for (QueryBuilder filter : filters) {
                boolQuery.should(filter);
            }
            boolQuery.minimumShouldMatch(1);
            return boolQuery;
        }
    }

    @Override
    public VertexQuery hasDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    @Override
    public VertexQuery hasOtherVertexId(String otherVertexId) {
        this.otherVertexId = otherVertexId;
        return this;
    }

    @Override
    public String toString() {
        return super.toString() +
                ", sourceVertex=" + sourceVertex +
                ", otherVertexId=" + otherVertexId +
                ", direction=" + direction;
    }
}
