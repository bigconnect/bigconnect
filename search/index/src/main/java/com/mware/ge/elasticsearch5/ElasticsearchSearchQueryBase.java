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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.mware.ge.*;
import com.mware.ge.elasticsearch5.sorting.ElasticsearchSortingStrategy;
import com.mware.ge.elasticsearch5.utils.ElasticsearchTypes;
import com.mware.ge.elasticsearch5.utils.InfiniteScrollIterable;
import com.mware.ge.elasticsearch5.utils.PagingIterable;
import com.mware.ge.query.Query;
import com.mware.ge.query.QueryBase;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.SortDirection;
import com.mware.ge.query.aggregations.*;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.sorting.SortingStrategy;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.JoinIterable;
import com.mware.ge.values.storable.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex.*;
import static com.mware.ge.elasticsearch5.utils.SearchResponseUtils.checkForFailures;
import static com.mware.ge.util.StreamUtils.stream;

public class ElasticsearchSearchQueryBase extends QueryBase {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(ElasticsearchSearchQueryBase.class);
    public static final GeLogger QUERY_LOGGER = GeLoggerFactory.getQueryLogger(Query.class);
    public static final String TOP_HITS_AGGREGATION_NAME = "__ge_top_hits";
    public static final String KEYWORD_UNMAPPED_TYPE = "keyword";
    public static final String AGGREGATION_METADATA_FIELD_NAME_KEY = "fieldName";

    private final Client client;
    private final StandardAnalyzer analyzer;
    private final IndexSelectionStrategy indexSelectionStrategy;
    private final int pageSize;
    private String shardId;
    private final int pagingLimit;
    private final TimeValue scrollKeepAlive;
    private final int termAggregationShardSize;
    private GeQueryBuilder queryBuilder;
    private GeQueryBuilderTransformer queryTransformer;

    public ElasticsearchSearchQueryBase(
            Client client,
            Graph graph,
            GeQueryBuilder queryBuilder,
            Options options,
            Authorizations authorizations
    ) {
        super(graph, queryBuilder, authorizations);
        this.client = client;
        this.queryBuilder = queryBuilder;
        this.pageSize = options.pageSize;
        this.indexSelectionStrategy = options.indexSelectionStrategy;
        this.scrollKeepAlive = options.scrollKeepAlive;
        this.pagingLimit = options.pagingLimit;
        this.analyzer = options.analyzer;
        this.termAggregationShardSize = options.termAggregationShardSize;
        this.queryTransformer = new GeQueryBuilderTransformer(getSearchIndex(), getGraph(), getAuthorizations(), queryBuilder, analyzer);
    }

    private SearchRequestBuilder buildQuery(EnumSet<ElasticsearchDocumentType> elementTypes, FetchHints fetchHints, boolean includeAggregations) {
        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("searching for: " + toString());
        }

        List<QueryBuilder> filters = getInternalFilters(elementTypes, fetchHints);
        // AND all filters
        BoolQueryBuilder filterBuilder = QueryBuilders.boolQuery();
        for (QueryBuilder filter : filters) {
            filterBuilder.must(filter);
        }

        String[] indicesToQuery = getIndexSelectionStrategy().getIndicesToQuery(this, elementTypes);
        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("indicesToQuery: %s", Joiner.on(", ").join(indicesToQuery));
        }

        if (getSearchIndex().shouldRefreshIndexOnQuery()) {
            getSearchIndex().getIndexRefreshTracker().refresh(client, indicesToQuery);
        }

        QueryBuilder query = queryTransformer.getElasticQuery(elementTypes);

        SearchRequestBuilder searchRequestBuilder = getClient()
                .prepareSearch(indicesToQuery)
                .setQuery(QueryBuilders.boolQuery().must(query).filter(filterBuilder))
                .storedFields(
                        Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME,
                        Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME,
                        Elasticsearch5SearchIndex.CONCEPT_TYPE_FIELD_NAME,
                        Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME,
                        Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME
                );

        if (!StringUtils.isEmpty(shardId))
            searchRequestBuilder.setPreference("_shards:" + shardId);

        if (fetchHints.equals(FetchHints.NONE)) {
            searchRequestBuilder.storedFields(
                    Elasticsearch5SearchIndex.OUT_VERTEX_ID_FIELD_NAME,
                    Elasticsearch5SearchIndex.IN_VERTEX_ID_FIELD_NAME,
                    Elasticsearch5SearchIndex.EDGE_LABEL_FIELD_NAME
            );
        }
        if (queryBuilder.getMinScore() != null) {
            searchRequestBuilder.setMinScore(queryBuilder.getMinScore().floatValue());
        }
        if (includeAggregations) {
            List<AggregationBuilder> aggs = getElasticsearchAggregations(getAggregations());
            for (AggregationBuilder aggregationBuilder : aggs) {
                searchRequestBuilder.addAggregation(aggregationBuilder);
            }
        }

        applySort(searchRequestBuilder);

        return searchRequestBuilder;
    }

    protected List<QueryBuilder> getInternalFilters(EnumSet<ElasticsearchDocumentType> elementTypes, FetchHints fetchHints) {
        List<QueryBuilder> filters = new ArrayList<>();
        if (elementTypes != null) {
            addElementTypeFilter(filters, elementTypes);
        }

        if (!fetchHints.isIncludeHidden()) {
            String[] hiddenVertexPropertyNames = getSearchIndex().getPropertyNames(getGraph(), HIDDEN_VERTEX_FIELD_NAME, getAuthorizations());
            if (hiddenVertexPropertyNames != null && hiddenVertexPropertyNames.length > 0) {
                BoolQueryBuilder elementIsNotHiddenQuery = QueryBuilders.boolQuery();
                for (String hiddenVertexPropertyName : hiddenVertexPropertyNames) {
                    elementIsNotHiddenQuery.mustNot(QueryBuilders.existsQuery(hiddenVertexPropertyName));
                }
                filters.add(elementIsNotHiddenQuery);
            }
        }

        if (elementTypes == null
                || elementTypes.contains(ElasticsearchDocumentType.EDGE_EXTENDED_DATA)
                || elementTypes.contains(ElasticsearchDocumentType.VERTEX_EXTENDED_DATA)
        ) {
            Elasticsearch5SearchIndex es = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) getGraph()).getSearchIndex();
            Collection<String> queryableVisibilities = es.getQueryableExtendedDataVisibilities(getGraph(), getAuthorizations());
            TermsQueryBuilder extendedDataVisibilitiesTerms = QueryBuilders.termsQuery(EXTENDED_DATA_TABLE_COLUMN_VISIBILITIES_FIELD_NAME, queryableVisibilities);

            if (elementTypes == null
                    || elementTypes.contains(ElasticsearchDocumentType.EDGE)
                    || elementTypes.contains(ElasticsearchDocumentType.VERTEX)
            ) {
                TermsQueryBuilder extendedDataTerms = QueryBuilders.termsQuery(ELEMENT_TYPE_FIELD_NAME,
                        Arrays.asList(ElasticsearchDocumentType.EDGE_EXTENDED_DATA.getKey(), ElasticsearchDocumentType.VERTEX_EXTENDED_DATA.getKey()));

                BoolQueryBuilder elementOrExtendedDataFilter = QueryBuilders.boolQuery();
                elementOrExtendedDataFilter.should(QueryBuilders.boolQuery().mustNot(extendedDataTerms));
                elementOrExtendedDataFilter.should(extendedDataVisibilitiesTerms);
                elementOrExtendedDataFilter.minimumShouldMatch(1);
                filters.add(elementOrExtendedDataFilter);
            } else {
                filters.add(extendedDataVisibilitiesTerms);
            }
        }

        Elasticsearch5SearchIndex es = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) getGraph()).getSearchIndex();
        Collection<String> fields = es.getQueryableElementTypeVisibilityPropertyNames(getGraph(), getAuthorizations());
        BoolQueryBuilder atLeastOneFieldExistsFilter = QueryBuilders.boolQuery();
        for (String field : fields) {
            atLeastOneFieldExistsFilter.should(new ExistsQueryBuilder(field));
        }
        atLeastOneFieldExistsFilter.minimumShouldMatch(1);
        filters.add(atLeastOneFieldExistsFilter);
        return filters;
    }

    protected void applySort(SearchRequestBuilder q) {
        AtomicBoolean sortedById = new AtomicBoolean(false);
        for (SortContainer sortContainer : getBuilder().getSortContainers()) {
            if (sortContainer instanceof PropertySortContainer) {
                applySortProperty(q, (PropertySortContainer) sortContainer, sortedById);
            } else if (sortContainer instanceof SortingStrategySortContainer) {
                applySortStrategy(q, (SortingStrategySortContainer) sortContainer);
            } else {
                throw new GeException("Unexpected sorting type: " + sortContainer.getClass().getName());
            }
        }
        q.addSort("_score", SortOrder.DESC);
        if (!sortedById.get()) {
            // If an id sort isn't specified, default is to sort by score and then sort id by ascending order after specified sorts
            q.addSort(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME, SortOrder.ASC);
        }
    }

    private void applySortStrategy(SearchRequestBuilder q, SortingStrategySortContainer sortContainer) {
        SortingStrategy sortingStrategy = sortContainer.sortingStrategy;
        if (!(sortingStrategy instanceof ElasticsearchSortingStrategy)) {
            throw new GeException(String.format(
                    "sorting strategies must implement %s to work with Elasticsearch",
                    ElasticsearchSortingStrategy.class.getName()
            ));
        }
        ((ElasticsearchSortingStrategy) sortingStrategy).updateElasticsearchQuery(
                getGraph(),
                getSearchIndex(),
                q,
                getAuthorizations(),
                sortContainer.direction
        );
    }

    protected void applySortProperty(SearchRequestBuilder q, PropertySortContainer sortContainer, AtomicBoolean sortedById) {
        SortOrder esOrder = sortContainer.direction == SortDirection.ASCENDING ? SortOrder.ASC : SortOrder.DESC;
        if (Element.ID_PROPERTY_NAME.equals(sortContainer.propertyName)) {
            q.addSort(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME, esOrder);
            sortedById.set(true);
        } else if (Edge.LABEL_PROPERTY_NAME.equals(sortContainer.propertyName)) {
            q.addSort(
                    SortBuilders.fieldSort(Elasticsearch5SearchIndex.EDGE_LABEL_FIELD_NAME)
                            .unmappedType(KEYWORD_UNMAPPED_TYPE)
                            .order(esOrder)
            );
        } else if (SearchIndex.CONCEPT_TYPE_FIELD_NAME.equals(sortContainer.propertyName)) {
            q.addSort(
                    SortBuilders.fieldSort(SearchIndex.CONCEPT_TYPE_FIELD_NAME)
                            .unmappedType(KEYWORD_UNMAPPED_TYPE)
                            .order(esOrder)
            );
        } else if (Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(sortContainer.propertyName)) {
            q.addSort(
                    SortBuilders.fieldSort(Elasticsearch5SearchIndex.OUT_VERTEX_ID_FIELD_NAME)
                            .unmappedType(KEYWORD_UNMAPPED_TYPE)
                            .order(esOrder)
            );
        } else if (Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(sortContainer.propertyName)) {
            q.addSort(
                    SortBuilders.fieldSort(Elasticsearch5SearchIndex.IN_VERTEX_ID_FIELD_NAME)
                            .unmappedType(KEYWORD_UNMAPPED_TYPE)
                            .order(esOrder)
            );
        } else if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(sortContainer.propertyName)) {
            throw new GeException("Cannot sort by " + Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME);
        } else if (ExtendedDataRow.TABLE_NAME.equals(sortContainer.propertyName)) {
            q.addSort(
                    SortBuilders.fieldSort(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME)
                            .unmappedType(KEYWORD_UNMAPPED_TYPE)
                            .order(esOrder)
            );
        } else if (ExtendedDataRow.ROW_ID.equals(sortContainer.propertyName)) {
            q.addSort(
                    SortBuilders.fieldSort(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME)
                            .unmappedType(KEYWORD_UNMAPPED_TYPE)
                            .order(esOrder)
            );
        } else if (ExtendedDataRow.ELEMENT_ID.equals(sortContainer.propertyName)) {
            q.addSort(
                    SortBuilders.fieldSort(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME)
                            .unmappedType(KEYWORD_UNMAPPED_TYPE)
                            .order(esOrder)
            );
        } else if (ExtendedDataRow.ELEMENT_TYPE.equals(sortContainer.propertyName)) {
            q.addSort(
                    SortBuilders.fieldSort(Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME)
                            .unmappedType(KEYWORD_UNMAPPED_TYPE)
                            .order(esOrder)
            );
        } else {
            PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(sortContainer.propertyName);
            if (propertyDefinition == null) {
                return;
            }
            if (!getSearchIndex().isPropertyInIndex(getGraph(), sortContainer.propertyName)) {
                return;
            }

            if (!propertyDefinition.isSortable()) {
                LOGGER.warn("Shoould not sort on non-sortable fields");
            }

            String[] propertyNames = getSearchIndex().getPropertyNames(getGraph(), propertyDefinition.getPropertyName(), getAuthorizations());
            if (propertyNames.length > 1) {
                String scriptSrc = "def fieldValues = []; for (def fieldName : params.fieldNames) { if(doc[fieldName].size() !=0) { fieldValues.addAll(doc[fieldName]); }} " +
                        "if (params.esOrder == 'asc') { Collections.sort(fieldValues); } else { Collections.sort(fieldValues, Collections.reverseOrder()); }";

                if (TextValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
                    scriptSrc += "return fieldValues.length > 0 ? (fieldValues[0] instanceof JodaCompatibleZonedDateTime ? fieldValues[0].getMillis() : fieldValues[0]) : (params.esOrder == 'asc' ? Character.toString(Character.MAX_VALUE) : '');";
                } else {
                    scriptSrc += "return fieldValues.length > 0 ? (fieldValues[0] instanceof JodaCompatibleZonedDateTime ? fieldValues[0].getMillis() : fieldValues[0]) : (params.esOrder == 'asc' ? Long.MAX_VALUE : Long.MIN_VALUE);";
                }

                List<String> fieldNames = Arrays.stream(propertyNames).map(propertyName ->
                        propertyName + (TextValue.class.isAssignableFrom(propertyDefinition.getDataType()) ? Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX : "")
                ).collect(Collectors.toList());
                HashMap<String, Object> scriptParams = new HashMap<>();
                scriptParams.put("fieldNames", fieldNames);
                scriptParams.put("esOrder", esOrder == SortOrder.DESC ? "desc" : "asc");
                scriptParams.put("dataType", propertyDefinition.getDataType().getSimpleName());
                Script script = new Script(ScriptType.INLINE, "painless", scriptSrc, scriptParams);
                ScriptSortBuilder.ScriptSortType sortType = TextValue.class.isAssignableFrom(propertyDefinition.getDataType()) ? ScriptSortBuilder.ScriptSortType.STRING : ScriptSortBuilder.ScriptSortType.NUMBER;
                q.addSort(SortBuilders.scriptSort(script, sortType)
                        .order(esOrder)
                        .sortMode(esOrder == SortOrder.DESC ? SortMode.MAX : SortMode.MIN));
            } else if (propertyNames.length == 1) {
                String sortField = propertyNames[0];
                String unmappedType = ElasticsearchTypes.fromJavaClass(propertyDefinition.getDataType());
                if (TextValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
                    sortField += Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
                    unmappedType = KEYWORD_UNMAPPED_TYPE;
                }
                q.addSort(
                        SortBuilders.fieldSort(sortField)
                                .unmappedType(unmappedType)
                                .order(esOrder)
                );
            }
        }
    }

    @Override
    public QueryResultsIterable<? extends GeObject> search(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints) {
        if (shouldUseScrollApi()) {
            return searchScroll(objectTypes, fetchHints);
        }
        return searchPaged(objectTypes, fetchHints);
    }

    private QueryResultsIterable<? extends GeObject> searchScroll(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints) {
        return new QueryInfiniteScrollIterable<GeObject>(objectTypes, fetchHints, getBuilder().getLimit()) {
            @Override
            protected ElasticsearchGraphQueryIterable<GeObject> searchResponseToIterable(SearchResponse searchResponse) {
                return ElasticsearchSearchQueryBase.this.searchResponseToGeObjectIterable(searchResponse, fetchHints);
            }

            @Override
            protected IdStrategy getIdStrategy() {
                return getSearchIndex().getIdStrategy();
            }
        };
    }

    private void closeScroll(String scrollId) {
        try {
            if (StringUtils.isEmpty(scrollId))
                return;;

            ClearScrollResponse clearScrollResponse = client.prepareClearScroll()
                    .addScrollId(scrollId)
                    .execute().actionGet();
            if (!clearScrollResponse.isSucceeded()) {
                LOGGER.warn("Unable to clear scroll " + scrollId);
            }
        } catch (Exception ex) {
            LOGGER.warn("Could not close iterator " + scrollId, ex);
        }
    }

    private QueryResultsIterable<? extends GeObject> searchPaged(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints) {
        return new PagingIterable<GeObject>(queryBuilder.getSkip(), queryBuilder.getLimit(), pageSize) {
            @Override
            protected ElasticsearchGraphQueryIterable<GeObject> getPageIterable(int skip, int limit, boolean includeAggregations) {
                SearchResponse response;
                try {
                    response = getSearchResponse(ElasticsearchDocumentType.fromGeObjectTypes(objectTypes), fetchHints, skip, limit, includeAggregations);
                } catch (IndexNotFoundException ex) {
                    LOGGER.debug("Index missing: %s (returning empty iterable)", ex.getMessage());
                    return createEmptyIterable();
                } catch (GeNoMatchingPropertiesException ex) {
                    LOGGER.debug("Could not find property: %s (returning empty iterable)", ex.getPropertyName());
                    return createEmptyIterable();
                }
                return searchResponseToGeObjectIterable(response, fetchHints);
            }
        };
    }

    private ElasticsearchGraphQueryIterable<GeObject> searchResponseToGeObjectIterable(SearchResponse response, FetchHints fetchHints) {
        final SearchHits hits = response.getHits();
        Ids ids = new Ids(getIdStrategy(), hits);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "elasticsearch results (vertices: %d + edges: %d + extended data: %d = %d)",
                    ids.getVertexIds().size(),
                    ids.getEdgeIds().size(),
                    ids.getExtendedDataIds().size(),
                    ids.getVertexIds().size() + ids.getEdgeIds().size() + ids.getExtendedDataIds().size()
            );
        }

        // since ES doesn't support security we will rely on the graph to provide edge filtering
        // and rely on the DefaultGraphQueryIterable to provide property filtering
        GeQueryBuilder filterParameters = getBuilder().clone();
        filterParameters.skip(0); // ES already did a skip
        List<Iterable<? extends GeObject>> items = new ArrayList<>();
        Authorizations authorizations = getAuthorizations();
        if (ids.getVertexIds().size() > 0) {
            if (fetchHints.equals(FetchHints.NONE)) {
                items.add(getElasticsearchVertices(hits, fetchHints, authorizations));
            } else {
                Iterable<? extends GeObject> vertices = getGraph().getVertices(ids.getVertexIds(), fetchHints, authorizations);
                items.add(vertices);
            }
        }
        if (ids.getEdgeIds().size() > 0) {
            if (fetchHints.equals(FetchHints.NONE)) {
                items.add(getElasticsearchEdges(hits, fetchHints, authorizations));
            } else {
                Iterable<? extends GeObject> edges = getGraph().getEdges(ids.getEdgeIds(), fetchHints, authorizations);
                items.add(edges);
            }
        }
        if (ids.getExtendedDataIds().size() > 0) {
            Iterable<? extends GeObject> extendedDataRows = getGraph().getExtendedData(ids.getExtendedDataIds(), fetchHints, authorizations);
            items.add(extendedDataRows);
        }
        Iterable<GeObject> geObjects = new JoinIterable<>(items);
        List<GeObject> sortedGeObjects = sortGeObjectsByResultOrder(geObjects, ids.getIds());

        // TODO instead of passing false here to not evaluate the query string it would be better to support the Lucene query
        return createIterable(response, sortedGeObjects, response.getTook().getMillis(), hits);
    }

    private QueryResultsIterable<SearchHit> searchHits(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints) {
        if (shouldUseScrollApi()) {
            return searchScrollHits(objectTypes, fetchHints);
        }
        return searchPagedHits(objectTypes, fetchHints);
    }

    private QueryInfiniteScrollIterable<SearchHit> searchScrollHits(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints) {
        return new QueryInfiniteScrollIterable<SearchHit>(objectTypes, fetchHints, getBuilder().getLimit()) {
            @Override
            protected ElasticsearchGraphQueryIterable<SearchHit> searchResponseToIterable(SearchResponse searchResponse) {
                return ElasticsearchSearchQueryBase.this.searchResponseToSearchHitsIterable(searchResponse);
            }

            @Override
            protected IdStrategy getIdStrategy() {
                return getSearchIndex().getIdStrategy();
            }
        };
    }

    private PagingIterable<SearchHit> searchPagedHits(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints) {
        return new PagingIterable<SearchHit>(getBuilder().getSkip(), getBuilder().getLimit(), pageSize) {
            @Override
            protected ElasticsearchGraphQueryIterable<SearchHit> getPageIterable(int skip, int limit, boolean includeAggregations) {
                SearchResponse response;
                try {
                    response = getSearchResponse(ElasticsearchDocumentType.fromGeObjectTypes(objectTypes), fetchHints, skip, limit, includeAggregations);
                } catch (IndexNotFoundException ex) {
                    LOGGER.debug("Index missing: %s (returning empty iterable)", ex.getMessage());
                    return createEmptyIterable();
                } catch (GeNoMatchingPropertiesException ex) {
                    LOGGER.debug("Could not find property: %s (returning empty iterable)", ex.getPropertyName());
                    return createEmptyIterable();
                }

                return searchResponseToSearchHitsIterable(response);
            }
        };
    }

    private ElasticsearchGraphQueryIterable<SearchHit> searchResponseToSearchHitsIterable(SearchResponse response) {
        SearchHits hits = response.getHits();
        Iterable<SearchHit> hitsIterable = IterableUtils.toIterable(hits.getHits());
        return createIterable(response, hitsIterable, response.getTook().getMillis(), hits);
    }

    private List<ElasticsearchVertex> getElasticsearchVertices(SearchHits hits, FetchHints fetchHints, Authorizations authorizations) {
        return stream(hits)
                .map(hit -> {
                    String elementId = hit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME).getValue();
                    String conceptType = hit.getFields().get(Elasticsearch5SearchIndex.CONCEPT_TYPE_FIELD_NAME).getValue();
                    return new ElasticsearchVertex(
                            getGraph(),
                            elementId,
                            conceptType,
                            fetchHints,
                            authorizations
                    );
                }).collect(Collectors.toList());
    }

    private List<ElasticsearchEdge> getElasticsearchEdges(SearchHits hits, FetchHints fetchHints, Authorizations authorizations) {
        return stream(hits)
                .map(hit -> {
                    String inVertexId = hit.getFields().get(Elasticsearch5SearchIndex.IN_VERTEX_ID_FIELD_NAME).getValue();
                    String outVertexId = hit.getFields().get(Elasticsearch5SearchIndex.OUT_VERTEX_ID_FIELD_NAME).getValue();
                    String label = hit.getFields().get(Elasticsearch5SearchIndex.EDGE_LABEL_FIELD_NAME).getValue();
                    String elementId = hit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME).getValue();
                    return new ElasticsearchEdge(
                            getGraph(),
                            elementId,
                            label,
                            inVertexId,
                            outVertexId,
                            fetchHints,
                            authorizations
                    );
                }).collect(Collectors.toList());
    }

    @Override
    public QueryResultsIterable<String> vertexIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new ElasticsearchGraphQueryIdIterable<>(getIdStrategy(), searchHits(EnumSet.of(GeObjectType.VERTEX), fetchHints));
    }

    @Override
    public QueryResultsIterable<String> edgeIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new ElasticsearchGraphQueryIdIterable<>(getIdStrategy(), searchHits(EnumSet.of(GeObjectType.EDGE), fetchHints));
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new ElasticsearchGraphQueryIdIterable<>(getIdStrategy(), searchHits(EnumSet.of(GeObjectType.EXTENDED_DATA), fetchHints));
    }

    @Override
    public QueryResultsIterable<String> elementIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new ElasticsearchGraphQueryIdIterable<>(getIdStrategy(), searchHits(GeObjectType.ELEMENTS, fetchHints));
    }

    private <T extends GeObject> List<T> sortGeObjectsByResultOrder(Iterable<T> geObjects, List<String> ids) {
        ImmutableMap<String, T> itemMap = Maps.uniqueIndex(geObjects, geObject -> {
            if (geObject instanceof Element) {
                return ((Element) geObject).getId();
            } else if (geObject instanceof ExtendedDataRow) {
                return ((ExtendedDataRow) geObject).getId().toString();
            } else {
                throw new GeException("Unhandled searchable item type: " + geObject.getClass().getName());
            }
        });

        List<T> results = new ArrayList<>();
        for (String id : ids) {
            T item = itemMap.get(id);
            if (item != null) {
                results.add(item);
            }
        }
        return results;
    }

    private <T> EmptyElasticsearchGraphQueryIterable<T> createEmptyIterable() {
        return new EmptyElasticsearchGraphQueryIterable<>(ElasticsearchSearchQueryBase.this);
    }

    protected <T> ElasticsearchGraphQueryIterable<T> createIterable(
            SearchResponse response,
            Iterable<T> geObjects,
            long searchTimeInMillis,
            SearchHits hits
    ) {
        return new ElasticsearchGraphQueryIterable<>(
                this,
                response,
                geObjects,
                hits.getTotalHits().value,
                searchTimeInMillis * 1000000,
                hits,
                getAuthorizations()
        );
    }

    private SearchResponse getSearchResponse(EnumSet<ElasticsearchDocumentType> elementType, FetchHints fetchHints, int skip, int limit, boolean includeAggregations) {
        SearchRequestBuilder q = buildQuery(elementType, fetchHints, includeAggregations)
                .setFrom(skip)
                .setSize(limit)
                .setTrackTotalHits(true);
        if (QUERY_LOGGER.isTraceEnabled()) {
            QUERY_LOGGER.trace("query: %s", q);
        }

        SearchResponse searchResponse = checkForFailures(q.execute().actionGet());
        if (LOGGER.isDebugEnabled()) {
            SearchHits hits = searchResponse.getHits();
            LOGGER.debug(
                    "elasticsearch results %d of %d (time: %dms) ** %s **",
                    hits.getHits().length,
                    hits.getTotalHits().value,
                    searchResponse.getTook().millis(),
                    q
            );
        }

        return searchResponse;
    }

    public Elasticsearch5SearchIndex getSearchIndex() {
        return (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) getGraph()).getSearchIndex();
    }

    protected void addElementTypeFilter(List<QueryBuilder> filters, EnumSet<ElasticsearchDocumentType> elementType) {
        if (elementType != null) {
            filters.add(createElementTypeFilter(elementType));
        }
    }

    protected TermsQueryBuilder createElementTypeFilter(EnumSet<ElasticsearchDocumentType> elementType) {
        List<String> values = new ArrayList<>();
        for (ElasticsearchDocumentType et : elementType) {
            values.add(et.getKey());
        }
        return QueryBuilders.termsQuery(
                Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME,
                values.toArray(new String[values.size()])
        );
    }

    public Client getClient() {
        return client;
    }

    @Override
    public boolean isAggregationSupported(Aggregation agg) {
        if (agg instanceof HistogramAggregation) {
            return true;
        }
        if (agg instanceof RangeAggregation) {
            return true;
        }
        if (agg instanceof PercentilesAggregation) {
            return true;
        }
        if (agg instanceof TermsAggregation) {
            return true;
        }
        if (agg instanceof GeohashAggregation) {
            return true;
        }
        if (agg instanceof StatisticsAggregation) {
            return true;
        }
        if (agg instanceof ChronoFieldAggregation) {
            return true;
        }
        if (agg instanceof CardinalityAggregation) {
            return true;
        }
        if (agg instanceof SumAggregation) {
            return true;
        }
        if (agg instanceof AvgAggregation) {
            return true;
        }
        if (agg instanceof MinAggregation) {
            return true;
        }
        if (agg instanceof MaxAggregation) {
            return true;
        }
        return false;
    }

    protected List<AggregationBuilder> getElasticsearchAggregations(Iterable<Aggregation> aggregations) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        for (Aggregation agg : aggregations) {
            if (agg instanceof HistogramAggregation) {
                aggs.addAll(getElasticsearchHistogramAggregations((HistogramAggregation) agg));
            } else if (agg instanceof RangeAggregation) {
                aggs.addAll(getElasticsearchRangeAggregations((RangeAggregation) agg));
            } else if (agg instanceof PercentilesAggregation) {
                aggs.addAll(getElasticsearchPercentilesAggregations((PercentilesAggregation) agg));
            } else if (agg instanceof TermsAggregation) {
                aggs.addAll(getElasticsearchTermsAggregations((TermsAggregation) agg));
            } else if (agg instanceof GeohashAggregation) {
                aggs.addAll(getElasticsearchGeohashAggregations((GeohashAggregation) agg));
            } else if (agg instanceof StatisticsAggregation) {
                aggs.addAll(getElasticsearchStatisticsAggregations((StatisticsAggregation) agg));
            } else if (agg instanceof ChronoFieldAggregation) {
                aggs.addAll(getElasticsearchChronoUnitAggregation((ChronoFieldAggregation) agg));
            } else if (agg instanceof CardinalityAggregation) {
                aggs.addAll(getElasticsearchCardinalityAggregations((CardinalityAggregation) agg));
            } else if (agg instanceof SumAggregation) {
                aggs.addAll(getElasticsearchSumAggregations((SumAggregation) agg));
            } else if (agg instanceof MaxAggregation) {
                aggs.addAll(getElasticsearchMaxAggregations((MaxAggregation) agg));
            } else if (agg instanceof MinAggregation) {
                aggs.addAll(getElasticsearchMinAggregations((MinAggregation) agg));
            } else if (agg instanceof AvgAggregation) {
                aggs.addAll(getElasticsearchAvgAggregations((AvgAggregation) agg));
            } else {
                throw new GeException("Could not add aggregation of type: " + agg.getClass().getName());
            }
        }
        return aggs;
    }

    protected List<AggregationBuilder> getElasticsearchGeohashAggregations(GeohashAggregation agg) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(agg.getFieldName());
        if (propertyDefinition == null) {
            throw new GeException("Unknown property " + agg.getFieldName() + " for geohash aggregation.");
        }
        if (!GeoPointValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
            throw new GeNotSupportedException("Only GeoPoint properties are valid for Geohash aggregation. Invalid property " + agg.getFieldName());
        }
        for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), agg.getFieldName(), getAuthorizations())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            GeoGridAggregationBuilder geoHashAgg = AggregationBuilders.geohashGrid(aggName);
            geoHashAgg.field(propertyName + Elasticsearch5SearchIndex.GEO_POINT_PROPERTY_NAME_SUFFIX);
            geoHashAgg.precision(agg.getPrecision());
            aggs.add(geoHashAgg);
        }
        return aggs;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchStatisticsAggregations(StatisticsAggregation agg) {
        List<AbstractAggregationBuilder> aggs = new ArrayList<>();
        for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), agg.getFieldName(), getAuthorizations())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            ExtendedStatsAggregationBuilder statsAgg = AggregationBuilders.extendedStats(aggName);
            statsAgg.field(propertyName);
            aggs.add(statsAgg);
        }
        return aggs;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchPercentilesAggregations(PercentilesAggregation agg) {
        String propertyName = getSearchIndex().addVisibilityToPropertyName(getGraph(), agg.getFieldName(), agg.getVisibility());
        String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
        String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
        PercentilesAggregationBuilder percentilesAgg = AggregationBuilders.percentiles(aggName);
        percentilesAgg.field(propertyName);
        if (agg.getPercents() != null && agg.getPercents().length > 0) {
            percentilesAgg.percentiles(agg.getPercents());
        }
        return Collections.singletonList(percentilesAgg);
    }

    private String createAggregationName(String aggName, String visibilityHash) {
        if (visibilityHash != null && visibilityHash.length() > 0) {
            return aggName + "_" + visibilityHash;
        }
        return aggName;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchSumAggregations(SumAggregation agg) {
        List<AbstractAggregationBuilder> aggs = new ArrayList<>();
        for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), agg.getFieldName(), getAuthorizations())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            SumAggregationBuilder statsAgg = AggregationBuilders.sum(aggName);
            statsAgg.field(propertyName);
            aggs.add(statsAgg);
        }
        return aggs;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchMaxAggregations(MaxAggregation agg) {
        List<AbstractAggregationBuilder> aggs = new ArrayList<>();
        for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), agg.getFieldName(), getAuthorizations())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            MaxAggregationBuilder statsAgg = AggregationBuilders.max(aggName);
            statsAgg.field(propertyName);
            aggs.add(statsAgg);
        }
        return aggs;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchMinAggregations(MinAggregation agg) {
        List<AbstractAggregationBuilder> aggs = new ArrayList<>();
        for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), agg.getFieldName(), getAuthorizations())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            MinAggregationBuilder statsAgg = AggregationBuilders.min(aggName);
            statsAgg.field(propertyName);
            aggs.add(statsAgg);
        }
        return aggs;
    }

    protected List<AbstractAggregationBuilder> getElasticsearchAvgAggregations(AvgAggregation agg) {
        List<AbstractAggregationBuilder> aggs = new ArrayList<>();
        for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), agg.getFieldName(), getAuthorizations())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            AvgAggregationBuilder statsAgg = AggregationBuilders.avg(aggName);
            statsAgg.field(propertyName);
            aggs.add(statsAgg);
        }
        return aggs;
    }

    protected List<AggregationBuilder> getElasticsearchCardinalityAggregations(CardinalityAggregation agg) {
        List<AggregationBuilder> cardinalityAggs = new ArrayList<>();
        String fieldName = agg.getPropertyName();
        if (Element.ID_PROPERTY_NAME.equals(fieldName)
                || Edge.LABEL_PROPERTY_NAME.equals(fieldName)
                || Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(fieldName)
                || Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(fieldName)
                || ExtendedDataRow.TABLE_NAME.equals(fieldName)
                || ExtendedDataRow.ROW_ID.equals(fieldName)
                || ExtendedDataRow.ELEMENT_ID.equals(fieldName)
                || ExtendedDataRow.ELEMENT_TYPE.equals(fieldName)) {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put(AGGREGATION_METADATA_FIELD_NAME_KEY, fieldName);

            if (Element.ID_PROPERTY_NAME.equals(fieldName) || ExtendedDataRow.ELEMENT_ID.equals(fieldName)) {
                fieldName = ELEMENT_ID_FIELD_NAME;
            } else if (ExtendedDataRow.ELEMENT_TYPE.equals(fieldName)) {
                fieldName = ELEMENT_TYPE_FIELD_NAME;
            }
            String aggregationName = createAggregationName(agg.getAggregationName(), "0");
            CardinalityAggregationBuilder cardinalityAgg = AggregationBuilders.cardinality(aggregationName);
            cardinalityAgg.setMetaData(metadata);
            cardinalityAgg.field(fieldName);
            cardinalityAggs.add(cardinalityAgg);
        } else {
            LOGGER.debug("Excuting Cardinality Aggregation on empty visibility only !");
            PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(fieldName);
            String[] propertyNames = getSearchIndex().getPropertyNames(getGraph(), fieldName, getAuthorizations());
            String propertyName = null;

            if (propertyNames.length == 1) {
                propertyName = propertyNames[0];
            } else if (propertyNames.length > 1) {
                PropertyNameVisibilitiesStore visibilitiesStore = getSearchIndex().getPropertyNameVisibilitiesStore();

                // get only the property with empty visibility
                boolean found = false;
                for (String p : propertyNames) {
                    String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(p);
                    String publicHash = visibilitiesStore.getHash(getGraph(), p, Visibility.EMPTY);
                    if (publicHash.equals(visibilityHash)) {
                        propertyName = p;
                        found = true;
                        break;
                    }
                }

                if (!found)
                    throw new GeException("Could not find a property with empty visibility: " + fieldName);
            }

            if (propertyName == null)
                throw new GeException("Could not compute the CardinalityAggregation property for: " + fieldName);

            boolean exactMatchProperty = queryTransformer.isExactMatchPropertyDefinition(propertyDefinition);
            String propertyNameWithSuffix;
            if (exactMatchProperty) {
                propertyNameWithSuffix = propertyName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
            } else {
                propertyNameWithSuffix = propertyName;
            }

            String aggregationName = createAggregationName(agg.getAggregationName(), "0");
            CardinalityAggregationBuilder cardinalityAgg = AggregationBuilders.cardinality(aggregationName);
            cardinalityAgg.field(propertyNameWithSuffix);
            cardinalityAggs.add(cardinalityAgg);
        }
        return cardinalityAggs;
    }

    protected List<AggregationBuilder> getElasticsearchTermsAggregations(TermsAggregation agg) {
        List<AggregationBuilder> termsAggs = new ArrayList<>();
        String fieldName = agg.getPropertyName();
        if (Edge.IN_OR_OUT_VERTEX_ID_PROPERTY_NAME.equals(fieldName)) {
            throw new GeException("Cannot aggregate by: " + fieldName);
        }

        if (Element.ID_PROPERTY_NAME.equals(fieldName)
                || SearchIndex.CONCEPT_TYPE_FIELD_NAME.equals(fieldName)
                || Edge.LABEL_PROPERTY_NAME.equals(fieldName)
                || Edge.OUT_VERTEX_ID_PROPERTY_NAME.equals(fieldName)
                || Edge.IN_VERTEX_ID_PROPERTY_NAME.equals(fieldName)
                || ExtendedDataRow.TABLE_NAME.equals(fieldName)
                || ExtendedDataRow.ROW_ID.equals(fieldName)
                || ExtendedDataRow.ELEMENT_ID.equals(fieldName)
                || ExtendedDataRow.ELEMENT_TYPE.equals(fieldName)) {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put(AGGREGATION_METADATA_FIELD_NAME_KEY, fieldName);

            if (Element.ID_PROPERTY_NAME.equals(fieldName) || ExtendedDataRow.ELEMENT_ID.equals(fieldName)) {
                fieldName = ELEMENT_ID_FIELD_NAME;
            } else if (ExtendedDataRow.ELEMENT_TYPE.equals(fieldName)) {
                fieldName = ELEMENT_TYPE_FIELD_NAME;
            }

            TermsAggregationBuilder termsAgg = AggregationBuilders.terms(createAggregationName(agg.getAggregationName(), "0"));
            termsAgg.setMetaData(metadata);
            termsAgg.field(fieldName);
            if (agg.getSize() != null) {
                termsAgg.size(agg.getSize());
            }
            termsAgg.shardSize(termAggregationShardSize);

            for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                termsAgg.subAggregation(subAgg);
            }

            termsAggs.add(termsAgg);
        } else {
            PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(fieldName);
            for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), fieldName, getAuthorizations())) {
                boolean exactMatchProperty = queryTransformer.isExactMatchPropertyDefinition(propertyDefinition);
                String propertyNameWithSuffix;
                if (exactMatchProperty) {
                    propertyNameWithSuffix = propertyName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
                } else {
                    propertyNameWithSuffix = propertyName;
                }

                String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyNameWithSuffix);
                String aggregationName = createAggregationName(agg.getAggregationName(), visibilityHash);
                TermsAggregationBuilder termsAgg = AggregationBuilders.terms(aggregationName);
                termsAgg.field(propertyNameWithSuffix);

                if (agg.getSize() != null) {
                    termsAgg.size(agg.getSize());
                }

                if (!StringUtils.isEmpty(agg.getExcluded())) {
                    termsAgg.includeExclude(new IncludeExclude(null, agg.getExcluded()));
                }

                termsAgg.shardSize(termAggregationShardSize);

                if (exactMatchProperty && propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                    termsAgg.subAggregation(
                            AggregationBuilders.topHits(TOP_HITS_AGGREGATION_NAME)
                                    .fetchSource(new String[]{propertyName}, new String[0])
                                    .size(1)
                    );
                }

                if (agg.getOrderBy() != null) {
                    Aggregation orderByAgg = agg.getOrderBy().aggregation;
                    if (orderByAgg instanceof AggregationWithFieldName) {
                        String orderByFieldName = ((AggregationWithFieldName) orderByAgg).getFieldName();
                        PropertyDefinition orderByFieldDef = getGraph().getPropertyDefinition(orderByFieldName);
                        boolean orderByExactMatchProp = queryTransformer.isExactMatchPropertyDefinition(orderByFieldDef);

                        List<BucketOrder> bucketOrders = new ArrayList<>();
                        for (String orderByPropName : getSearchIndex().getPropertyNames(getGraph(), orderByFieldName, getAuthorizations())) {
                            String orderByPropNameWithSuffix;
                            if (orderByExactMatchProp) {
                                orderByPropNameWithSuffix = orderByPropName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX;
                            } else {
                                orderByPropNameWithSuffix = orderByPropName;
                            }
                            String orderByPropVisibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(orderByPropNameWithSuffix);
                            String ordeByAggregationName = createAggregationName(orderByAgg.getAggregationName(), orderByPropVisibilityHash);
                            bucketOrders.add(BucketOrder.aggregation(ordeByAggregationName, agg.getOrderBy().direction.equals(SortDirection.ASCENDING)));
                        }

                        termsAgg.order(BucketOrder.compound(bucketOrders));
                    }
                }

                List<AggregationBuilder> nestedAggs = getElasticsearchAggregations(agg.getNestedAggregations());
                for (AggregationBuilder subAgg : nestedAggs) {
                    termsAgg.subAggregation(subAgg);
                }

                termsAggs.add(termsAgg);
            }
        }
        return termsAggs;
    }

    private Collection<? extends AggregationBuilder> getElasticsearchChronoUnitAggregation(ChronoFieldAggregation agg) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(agg.getPropertyName());
        if (propertyDefinition == null) {
            throw new GeException("Could not find mapping for property: " + agg.getPropertyName());
        }
        Class<? extends Value> propertyDataType = propertyDefinition.getDataType();
        for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), agg.getPropertyName(), getAuthorizations())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            if (DateValue.class.isAssignableFrom(propertyDataType) || DateTimeValue.class.isAssignableFrom(propertyDataType)) {
                HistogramAggregationBuilder histAgg = AggregationBuilders.histogram(aggName);
                histAgg.interval(1);
                if (agg.getMinDocumentCount() != null) {
                    histAgg.minDocCount(agg.getMinDocumentCount());
                } else {
                    histAgg.minDocCount(1L);
                }
                Script script = new Script(
                        ScriptType.INLINE,
                        "painless",
                        getChronoUnitAggregationScript(agg),
                        ImmutableMap.of(
                                "tzId", agg.getTimeZone().getID(),
                                "fieldName", propertyName,
                                "chronoField", agg.getChronoField().name())
                );
                histAgg.script(script);

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    histAgg.subAggregation(subAgg);
                }

                aggs.add(histAgg);
            } else {
                throw new GeException("Only dates are supported for hour of day aggregations");
            }
        }
        return aggs;
    }

    private String getChronoUnitAggregationScript(ChronoFieldAggregation agg) {
        String prefix = "def d = doc[params.fieldName]; " +
                "ZoneId zone = ZoneId.of(params.tzId); " +
                "if (d == null || d.size() == 0) { return -1; }" +
                "d = Instant.ofEpochMilli(d.value.millis).atZone(zone);";
        switch (agg.getChronoField()) {
            case DAY_OF_MONTH:
                return prefix + "return d.get(ChronoField.DAY_OF_MONTH);";
            case DAY_OF_WEEK:
                return prefix + "d = d.get(ChronoField.DAY_OF_WEEK);";
            case HOUR_OF_DAY:
                return prefix + "return d.get(ChronoField.HOUR_OF_DAY);";
            case MONTH_OF_YEAR:
                return prefix + "return d.get(ChronoField.MONTH_OF_YEAR);";
            case YEAR:
                return prefix + "d.get(ChronoField.YEAR);";
            default:
                return prefix + "return d.get(ChronoField.valueOf(params.chronoField));";
        }
    }

    protected List<AggregationBuilder> getElasticsearchHistogramAggregations(HistogramAggregation agg) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(agg.getFieldName());
        if (propertyDefinition == null) {
            throw new GeException("Could not find mapping for property: " + agg.getFieldName());
        }
        Class<? extends Value> propertyDataType = propertyDefinition.getDataType();
        for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), agg.getFieldName(), getAuthorizations())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            if (DateValue.class.isAssignableFrom(propertyDataType) || DateTimeValue.class.isAssignableFrom(propertyDataType)) {
                DateHistogramAggregationBuilder dateAgg = AggregationBuilders.dateHistogram(aggName);
                dateAgg.field(propertyName);
                String interval = agg.getInterval();
                if (interval == null || interval.isEmpty()) {
                    throw new GeException("Histogram Aggregation Interval cannot be null for " + propertyName);
                }

                if (Pattern.matches("^[0-9\\.]+$", interval)) {
                    interval += "ms";
                }

                DateHistogramInterval histogramInterval = new DateHistogramInterval(interval);
                try {
                    TimeValue.parseTimeValue(histogramInterval.toString(), null, getClass().getSimpleName() + ".interval");
                    dateAgg.fixedInterval(histogramInterval);
                } catch (IllegalArgumentException e) {
                    dateAgg.calendarInterval(histogramInterval);
                }
                dateAgg.minDocCount(1L);
                dateAgg.order(BucketOrder.count(false));
                if (agg.getMinDocumentCount() != null) {
                    dateAgg.minDocCount(agg.getMinDocumentCount());
                }
                if (agg.getExtendedBounds() != null) {
                    HistogramAggregation.ExtendedBounds<?> bounds = agg.getExtendedBounds();
                    if (LongValue.class.isAssignableFrom(bounds.getMinMaxType())) {
                        dateAgg.extendedBounds(new ExtendedBounds(
                                ((LongValue)bounds.getMin()).asObjectCopy(), ((LongValue) bounds.getMax()).asObjectCopy())
                        );
                    } else if (DateValue.class.isAssignableFrom(bounds.getMinMaxType())) {
                        dateAgg.extendedBounds(new ExtendedBounds(
                                ((DateValue) bounds.getMin()).asObjectCopy().atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli(),
                                ((DateValue) bounds.getMax()).asObjectCopy().atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()
                        ));
                    }  else if (DateTimeValue.class.isAssignableFrom(bounds.getMinMaxType())) {
                        dateAgg.extendedBounds(new ExtendedBounds(
                                ((DateTimeValue) bounds.getMin()).asObjectCopy().toInstant().toEpochMilli(),
                                ((DateTimeValue) bounds.getMax()).asObjectCopy().toInstant().toEpochMilli()
                        ));
                    } else if (bounds.getMinMaxType().isAssignableFrom(TextValue.class)) {
                        dateAgg.extendedBounds(
                                new ExtendedBounds(((TextValue) bounds.getMin()).stringValue(), ((TextValue) bounds.getMax()).stringValue())
                        );
                    } else {
                        throw new GeException("Unhandled extended bounds type. Expected Long, String, or Date. Found: " + bounds.getMinMaxType().getName());
                    }
                }

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    dateAgg.subAggregation(subAgg);
                }

                aggs.add(dateAgg);
            } else {
                HistogramAggregationBuilder histogramAgg = AggregationBuilders.histogram(aggName);
                histogramAgg.field(propertyName);
                if (agg.getInterval() != null && !agg.getInterval().trim().isEmpty()) {
                    histogramAgg.interval(Double.parseDouble(agg.getInterval()));
                } else {
                    continue;
                }
                histogramAgg.minDocCount(1L);
                histogramAgg.order(BucketOrder.count(false));

                if (agg.getMinDocumentCount() != null) {
                    histogramAgg.minDocCount(agg.getMinDocumentCount());
                }
                if (agg.getExtendedBounds() != null) {
                    HistogramAggregation.ExtendedBounds<?> bounds = agg.getExtendedBounds();
                    if (LongValue.class.isAssignableFrom(bounds.getMinMaxType())) {
                        histogramAgg.extendedBounds(((LongValue)bounds.getMin()).asObjectCopy(), ((LongValue) bounds.getMax()).asObjectCopy());
                    } else if (DoubleValue.class.isAssignableFrom(bounds.getMinMaxType())) {
                        histogramAgg.extendedBounds(((DoubleValue)bounds.getMin()).asObjectCopy(), ((DoubleValue) bounds.getMax()).asObjectCopy());
                    } else {
                        throw new GeException("Unhandled extended bounds type. Expected Double or Long. Found: " + bounds.getMinMaxType().getName());
                    }
                }

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    histogramAgg.subAggregation(subAgg);
                }

                aggs.add(histogramAgg);
            }
        }
        return aggs;
    }

    protected List<AggregationBuilder> getElasticsearchRangeAggregations(RangeAggregation agg) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        PropertyDefinition propertyDefinition = getGraph().getPropertyDefinition(agg.getFieldName());
        if (propertyDefinition == null) {
            throw new GeException("Could not find mapping for property: " + agg.getFieldName());
        }
        Class<? extends Value> propertyDataType = propertyDefinition.getDataType();
        for (String propertyName : getSearchIndex().getPropertyNames(getGraph(), agg.getFieldName(), getAuthorizations())) {
            String visibilityHash = getSearchIndex().getPropertyVisibilityHashFromPropertyName(propertyName);
            String aggName = createAggregationName(agg.getAggregationName(), visibilityHash);
            if (DateValue.class.isAssignableFrom(propertyDataType) || DateTimeValue.class.isAssignableFrom(propertyDataType)) {
                DateRangeAggregationBuilder dateRangeBuilder = AggregationBuilders.dateRange(aggName);
                dateRangeBuilder.field(propertyName);

                if (!Strings.isNullOrEmpty(agg.getFormat())) {
                    dateRangeBuilder.format(agg.getFormat());
                }

                for (RangeAggregation.Range range : agg.getRanges()) {
                    applyRange(dateRangeBuilder, range);
                }

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    dateRangeBuilder.subAggregation(subAgg);
                }

                aggs.add(dateRangeBuilder);
            } else {
                RangeAggregationBuilder rangeBuilder = AggregationBuilders.range(aggName);
                rangeBuilder.field(propertyName);

                if (!Strings.isNullOrEmpty(agg.getFormat())) {
                    throw new GeException("Invalid use of format for property: " + agg.getFieldName() +
                            ". Format is only valid for date properties");
                }

                for (RangeAggregation.Range range : agg.getRanges()) {
                    Value from = range.getFrom();
                    Value to = range.getTo();
                    if ((from != null && !(from instanceof NumberValue)) || (to != null && !(to instanceof NumberValue))) {
                        throw new GeException("Invalid range for property: " + agg.getFieldName() +
                                ". Both to and from must be Numeric.");
                    }
                    rangeBuilder.addRange(
                            range.getKey(),
                            from == null ? Double.MIN_VALUE : ((NumberValue) from).doubleValue(),
                            to == null ? Double.MAX_VALUE : ((NumberValue) to).doubleValue()
                    );
                }

                for (AggregationBuilder subAgg : getElasticsearchAggregations(agg.getNestedAggregations())) {
                    rangeBuilder.subAggregation(subAgg);
                }

                aggs.add(rangeBuilder);
            }
        }
        return aggs;
    }

    private void applyRange(DateRangeAggregationBuilder dateRangeBuilder, RangeAggregation.Range range) {
        Value from = range.getFrom();
        Value to = range.getTo();
        if ((from == null || from instanceof TextValue) && (to == null || to instanceof TextValue)) {
            String fromString = from != null ? ((TextValue) from).stringValue() : null;
            String toString = to != null ? ((TextValue) to).stringValue() : null;
            dateRangeBuilder.addRange(range.getKey(), fromString, toString);
        } else if ((from == null || from instanceof NumberValue) && (to == null || to instanceof NumberValue)) {
            double fromDouble = from == null ? null : ((NumberValue) from).doubleValue();
            double toDouble = to == null ? null : ((NumberValue) to).doubleValue();
            dateRangeBuilder.addRange(range.getKey(), fromDouble, toDouble);
        } else if ((from == null || from instanceof DateTimeValue) && (to == null || to instanceof DateTimeValue)) {
            ZonedDateTime fromDateTime = from != null ? (ZonedDateTime) from.asObjectCopy() : null;
            ZonedDateTime toDateTime = to != null ? (ZonedDateTime) to.asObjectCopy() : null;
            dateRangeBuilder.addRange(range.getKey(), fromDateTime, toDateTime);
        } else if ((from == null || from instanceof DateValue) && (to == null || to instanceof DateValue)) {
            ZonedDateTime fromDateTime = from == null ? null : ((LocalDate)from.asObjectCopy()).atStartOfDay(ZoneOffset.UTC);
            ZonedDateTime toDateTime = to == null ? null : ((LocalDate)to.asObjectCopy()).atStartOfDay(ZoneOffset.UTC);
            dateRangeBuilder.addRange(range.getKey(), fromDateTime, toDateTime);
        } else {
            String fromClassName = from == null ? null : from.getClass().getName();
            String toClassName = to == null ? null : to.getClass().getName();
            throw new GeException("unhandled range types " + fromClassName + ", " + toClassName);
        }
    }

    private boolean shouldUseScrollApi() {
        return getBuilder().getSkip() == 0 && (getBuilder().getLimit() == null || getBuilder().getLimit() > pagingLimit);
    }

    protected IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    public String getAggregationName(String name) {
        return getSearchIndex().getAggregationName(name);
    }

    public IdStrategy getIdStrategy() {
        return getSearchIndex().getIdStrategy();
    }

    @Override
    protected FetchHints idFetchHintsToElementFetchHints(EnumSet<IdFetchHint> idFetchHints) {
        return idFetchHints.contains(IdFetchHint.INCLUDE_HIDDEN)
                ? FetchHints.builder().setIncludeHidden(true).build()
                : FetchHints.NONE;
    }

    @Override
    public Query setShard(String shardId) {
        this.shardId = shardId;
        return this;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
                "parameters=" + getBuilder() +
                ", pageSize=" + pageSize +
                '}';
    }

    private abstract class QueryInfiniteScrollIterable<T> extends InfiniteScrollIterable<T> {
        private final EnumSet<GeObjectType> objectTypes;
        private final FetchHints fetchHints;

        public QueryInfiniteScrollIterable(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints, Long limit) {
            super(limit);
            this.objectTypes = objectTypes;
            this.fetchHints = fetchHints;
        }

        @Override
        protected SearchResponse getInitialSearchResponse() {
            try {
                SearchRequestBuilder q = buildQuery(ElasticsearchDocumentType.fromGeObjectTypes(objectTypes), fetchHints, true)
                        .setSize(pageSize)
                        .setScroll(scrollKeepAlive)
                        .setTrackTotalHits(true);
                if (QUERY_LOGGER.isTraceEnabled()) {
                    QUERY_LOGGER.trace("query: %s", q);
                }

                SearchResponse searchResponse = checkForFailures(q.execute().actionGet());
                SearchHits hits = searchResponse.getHits();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "elasticsearch results %d of %d (time: %dms) ** %s **",
                            hits.getHits().length,
                            hits.getTotalHits().value,
                            searchResponse.getTook().millis(),
                            q
                    );

                    if (searchResponse.getTook().millis() > 5000) {
                        new Exception().printStackTrace();
                    }
                }
                return searchResponse;
            } catch (IndexNotFoundException ex) {
                LOGGER.debug("Index missing: %s (returning empty iterable)", ex.getMessage());
                return null;
            } catch (GeNoMatchingPropertiesException ex) {
                LOGGER.debug("Could not find property: %s (returning empty iterable)", ex.getPropertyName());
                return null;
            }
        }

        @Override
        protected SearchResponse getNextSearchResponse(String scrollId) {
            try {
                return client.prepareSearchScroll(scrollId)
                        .setScroll(scrollKeepAlive)
                        .execute().actionGet();
            } catch (Exception ex) {
                throw new GeException("Failed to request more items from scroll " + scrollId, ex);
            }
        }

        @Override
        protected void closeScroll(String scrollId) {
            ElasticsearchSearchQueryBase.this.closeScroll(scrollId);
        }
    }

    private static class Ids {
        private final List<String> vertexIds;
        private final List<String> edgeIds;
        private final List<String> ids;
        private final List<ExtendedDataRowId> extendedDataIds;

        public Ids(IdStrategy idStrategy, SearchHits hits) {
            vertexIds = new ArrayList<>();
            edgeIds = new ArrayList<>();
            extendedDataIds = new ArrayList<>();
            ids = new ArrayList<>();
            for (SearchHit hit : hits) {
                ElasticsearchDocumentType dt = ElasticsearchDocumentType.fromSearchHit(hit);
                if (dt == null) {
                    continue;
                }
                switch (dt) {
                    case VERTEX:
                        String vertexId = idStrategy.vertexIdFromSearchHit(hit);
                        ids.add(vertexId);
                        vertexIds.add(vertexId);
                        break;
                    case EDGE:
                        String edgeId = idStrategy.edgeIdFromSearchHit(hit);
                        ids.add(edgeId);
                        edgeIds.add(edgeId);
                        break;
                    case VERTEX_EXTENDED_DATA:
                    case EDGE_EXTENDED_DATA:
                        ExtendedDataRowId extendedDataRowId = idStrategy.extendedDataRowIdFromSearchHit(hit);
                        ids.add(extendedDataRowId.toString());
                        extendedDataIds.add(extendedDataRowId);
                        break;
                    default:
                        LOGGER.warn("Unhandled document type: %s", dt);
                        break;
                }
            }
        }

        public List<String> getVertexIds() {
            return vertexIds;
        }

        public List<String> getEdgeIds() {
            return edgeIds;
        }

        public List<String> getIds() {
            return ids;
        }

        public List<ExtendedDataRowId> getExtendedDataIds() {
            return extendedDataIds;
        }
    }

    @SuppressWarnings("unused")
    public static class Options {
        public int pageSize;
        public IndexSelectionStrategy indexSelectionStrategy;
        public TimeValue scrollKeepAlive;
        public StandardAnalyzer analyzer = new StandardAnalyzer();
        public int pagingLimit;
        public int termAggregationShardSize;

        public int getPageSize() {
            return pageSize;
        }

        public Options setPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public IndexSelectionStrategy getIndexSelectionStrategy() {
            return indexSelectionStrategy;
        }

        public Options setIndexSelectionStrategy(IndexSelectionStrategy indexSelectionStrategy) {
            this.indexSelectionStrategy = indexSelectionStrategy;
            return this;
        }

        public TimeValue getScrollKeepAlive() {
            return scrollKeepAlive;
        }

        public Options setScrollKeepAlive(TimeValue scrollKeepAlive) {
            this.scrollKeepAlive = scrollKeepAlive;
            return this;
        }

        public StandardAnalyzer getAnalyzer() {
            return analyzer;
        }

        public Options setAnalyzer(StandardAnalyzer analyzer) {
            this.analyzer = analyzer;
            return this;
        }

        public int getPagingLimit() {
            return pagingLimit;
        }

        public Options setPagingLimit(int pagingLimit) {
            this.pagingLimit = pagingLimit;
            return this;
        }

        public int getTermAggregationShardSize() {
            return termAggregationShardSize;
        }

        public Options setTermAggregationShardSize(int termAggregationShardSize) {
            this.termAggregationShardSize = termAggregationShardSize;
            return this;
        }
    }
}

