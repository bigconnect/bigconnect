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
 *
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
package com.mware.core.model.search;

import com.mware.core.config.Configuration;
import com.mware.core.config.options.CoreOptions;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.SearchSchema;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.trace.Trace;
import com.mware.core.trace.TraceSpan;
import com.mware.core.user.User;
import com.mware.core.util.ClientApiConverter;
import com.mware.core.util.JSONUtil;
import com.mware.ge.*;
import com.mware.ge.query.*;
import com.mware.ge.query.aggregations.*;
import com.mware.ge.query.aggregations.SupportOrderByAggregation.AggregationSortContainer;
import com.mware.ge.query.builder.BoolQueryBuilder;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.time.Clocks;
import com.mware.ge.values.storable.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Precision;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.text.ParseException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static com.mware.ge.query.builder.GeQueryBuilders.*;
import static com.mware.ge.values.storable.DateTimeValue.datetime;
import static com.mware.ge.values.storable.Values.*;

public abstract class GeObjectSearchRunnerBase extends SearchRunner {
    protected final Graph graph;
    protected final SchemaRepository schemaRepository;
    protected long defaultSearchResultCount;
    private final static TextValue EMPTY_REFINEMENT_STRING = Values.stringValue("N/A");

    protected GeObjectSearchRunnerBase(
            SchemaRepository schemaRepository,
            Graph graph,
            Configuration configuration
    ) {
        this.schemaRepository = schemaRepository;
        this.graph = graph;
        this.defaultSearchResultCount = configuration.get(CoreOptions.DEFAULT_SEARCH_RESULT_COUNT);
    }

    @Override
    public QueryResultsIterableSearchResults run(
            SearchOptions searchOptions,
            User user,
            Authorizations authorizations
    ) {
        JSONArray filterJson = getFilterJson(searchOptions, searchOptions.getWorkspaceId());
        JSONArray sourceFilterJson = getSourceFilterJson(searchOptions);

        BoolQueryBuilder queryBuilder = getQuery(searchOptions, authorizations);
        Query query;
        Long size = 0L, offset = 0L;

        if (queryBuilder != null) {
            applyFiltersToQuery(queryBuilder, filterJson, searchOptions);
            applyConceptTypeFilterToQuery(queryBuilder, searchOptions);
            applyRefinementsToQuery(queryBuilder, searchOptions);
            applyEdgeLabelFilterToQuery(queryBuilder, searchOptions);
            applyExtendedDataFilters(queryBuilder, searchOptions);
            if (sourceFilterJson != null) {
                applyFiltersToQuery(queryBuilder, sourceFilterJson, searchOptions);
            }

            size = searchOptions.getOptionalParameter("size", defaultSearchResultCount);
            if (size != null && size != -1) {
                queryBuilder.limit(size);
            }

            offset = searchOptions.getOptionalParameter("offset", 0L);
            if (offset != null) {
                queryBuilder.skip(offset.intValue());
            }

            applySortToQuery(queryBuilder, searchOptions);

            query = graph.query(queryBuilder, authorizations);
            applyAggregationsToQuery(query, searchOptions);
            if(searchOptions.getOptionalParameter("includeFacets", Boolean.FALSE)) {
                applyDefaultAggregationsToQuery(query, searchOptions.getWorkspaceId());
            }

        } else {
            query = new EmptyResultsGraphQuery();
        }

        try (QueryResultsIterable<? extends GeObject> searchResults = getSearchResults(query, getFetchHints(searchOptions))) {
            if (searchResults == null) {
                throw new BcException("Failed to extract search results.");
            }
            return new QueryResultsIterableSearchResults(searchResults, query, offset, size);
        } catch(Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private FetchHints getFetchHints(SearchOptions searchOptions) {
        String fetchHintsString = searchOptions.getOptionalParameter("fetchHints", String.class);
        if (fetchHintsString == null) {
            return ClientApiConverter.SEARCH_FETCH_HINTS;
        }

        return FetchHintsBuilder.parse(new JSONObject(fetchHintsString)).build();
    }

    private void applyExtendedDataFilters(BoolQueryBuilder query, SearchOptions searchOptions) {
        String[] filterStrings = searchOptions.getOptionalParameter("extendedDataFilters[]", String[].class);
        if (filterStrings == null || filterStrings.length == 0) {
            return;
        }

        for (String filterString : filterStrings) {
            JSONObject filterJson = new JSONObject(filterString);
            String elementTypeString = filterJson.optString("elementType");
            ElementType elementType = elementTypeString == null ? null : ElementType.valueOf(elementTypeString);
            String elementId = filterJson.optString("elementId");
            String tableName = filterJson.optString("tableName");
            query.and(GeQueryBuilders.hasExtendedData(elementType, elementId, tableName));
        }
    }

    private void applyAggregationsToQuery(Query query, SearchOptions searchOptions) {
        String[] aggregates = searchOptions.getOptionalParameter("aggregations[]", String[].class);
        if (aggregates == null) {
            return;
        }
        for (String aggregate : aggregates) {
            JSONObject aggregateJson = new JSONObject(aggregate);
            Aggregation aggregation = getAggregation(aggregateJson);
            query.addAggregation(aggregation);
        }
    }

    private void applyDefaultAggregationsToQuery(Query query, String workspaceId) {
        // always add conceptType aggregation
        TermsAggregation ctAgg = new TermsAggregation("Concept", SearchIndex.CONCEPT_TYPE_FIELD_NAME);
        ctAgg.setMissingValue(EMPTY_REFINEMENT_STRING);
        query.addAggregation(ctAgg);

        for(SchemaProperty prop : schemaRepository.getProperties(workspaceId)) {
            boolean showFacet = prop.getSearchFacet();
            Aggregation aggregation;
            if (showFacet) {
                String aggregationName = prop.getDisplayName();
                String type = prop.getAggType();
                String field = prop.getName();

                if (type == null) {
                    throw new BcException("Aggregation type cannot be null for property " + prop.getName());
                }

                switch (type) {
                    case "term":
                        aggregation = new TermsAggregation(aggregationName, field);
                        ((TermsAggregation)aggregation).setMissingValue(EMPTY_REFINEMENT_STRING);
                        break;
                    case "geohash":
                        aggregation = new GeohashAggregation(aggregationName, field, prop.getAggPrecision());
                        break;
                    case "histogram":
                        aggregation = new HistogramAggregation(aggregationName, field, prop.getAggInterval(), prop.getAggMinDocumentCount());
                        ((HistogramAggregation)aggregation).setMissingValue(EMPTY_REFINEMENT_STRING);
                        break;
                    case "statistics":
                        aggregation = new StatisticsAggregation(aggregationName, field);
                        break;
                    case "calendar":
                        TimeZone timeZone = prop.getAggTimeZone() == null ? TimeZone.getDefault() : TimeZone.getTimeZone(prop.getAggTimeZone());
                        ChronoField chronoField = getChronoField(prop.getAggCalendarField());
                        aggregation = new ChronoFieldAggregation(aggregationName, field, prop.getAggMinDocumentCount(), timeZone, chronoField);
                        break;
                    default:
                        throw new BcException("Invalid aggregation type: " + type +" for property: "+field);
                }

                query.addAggregation(aggregation);
            }
        }
    }

    private Aggregation getAggregation(JSONObject aggregateJson) {
        String aggregationName = aggregateJson.getString("name");
        String type = aggregateJson.getString("type");
        Aggregation aggregation;
        switch (type) {
            case "term":
                aggregation = getTermsAggregation(aggregationName, aggregateJson);
                break;
            case "geohash":
                aggregation = getGeohashAggregation(aggregationName, aggregateJson);
                break;
            case "histogram":
                aggregation = getHistogramAggregation(aggregationName, aggregateJson);
                break;
            case "statistics":
                aggregation = getStatisticsAggregation(aggregationName, aggregateJson);
                break;
            case "calendar":
                aggregation = getCalendarFieldAggregation(aggregationName, aggregateJson);
                break;
            case "sum":
                aggregation = getSumAggregation(aggregationName, aggregateJson);
                break;
            case "avg":
                aggregation = getAvgAggregation(aggregationName, aggregateJson);
                break;
            case "min":
                aggregation = getMinAggregation(aggregationName, aggregateJson);
                break;
            case "max":
                aggregation = getMaxAggregation(aggregationName, aggregateJson);
                break;
            default:
                throw new BcException("Invalid aggregation type: " + type);
        }

        return addNestedAggregations(aggregation, aggregateJson);
    }

    private Aggregation addNestedAggregations(Aggregation aggregation, JSONObject aggregateJson) {
        JSONArray nestedAggregates = aggregateJson.optJSONArray("nested");
        if (nestedAggregates != null && nestedAggregates.length() > 0) {
            if (!(aggregation instanceof SupportsNestedAggregationsAggregation)) {
                throw new BcException("Aggregation does not support nesting: " + aggregation.getClass().getName());
            }
            for (int i = 0; i < nestedAggregates.length(); i++) {
                JSONObject nestedAggregateJson = nestedAggregates.getJSONObject(i);
                Aggregation nestedAggregate = getAggregation(nestedAggregateJson);
                if (nestedAggregateJson.has("orderBy") && aggregation instanceof SupportOrderByAggregation) {
                    SortDirection sortDirection = SortDirection.valueOf(nestedAggregateJson.getString("orderBy"));
                    ((SupportOrderByAggregation)aggregation).setOrderBy(new AggregationSortContainer(nestedAggregate, sortDirection));
                }
                ((SupportsNestedAggregationsAggregation) aggregation).addNestedAggregation(nestedAggregate);
            }
        }

        return aggregation;
    }

    private Aggregation getTermsAggregation(String aggregationName, JSONObject aggregateJson) {
        String field = aggregateJson.getString("field");
        TermsAggregation terms = new TermsAggregation(aggregationName, field);
        int size = aggregateJson.optInt("size", 0);
        if (size > 0) {
            terms.setSize(size);
        }
        String excluded = aggregateJson.optString("excluded", "");
        if (!StringUtils.isEmpty(excluded)) {
            terms.setExcluded(excluded);
        }

        return terms;
    }

    private Aggregation getGeohashAggregation(String aggregationName, JSONObject aggregateJson) {
        String field = aggregateJson.getString("field");
        int precision = aggregateJson.getInt("precision");
        return new GeohashAggregation(aggregationName, field, precision);
    }

    private Aggregation getHistogramAggregation(String aggregationName, JSONObject aggregateJson) {
        String field = aggregateJson.getString("field");
        String interval = aggregateJson.getString("interval");
        Long minDocumentCount = JSONUtil.getOptionalLong(aggregateJson, "minDocumentCount");
        return new HistogramAggregation(aggregationName, field, interval, minDocumentCount);
    }

    private Aggregation getStatisticsAggregation(String aggregationName, JSONObject aggregateJson) {
        String field = aggregateJson.getString("field");
        return new StatisticsAggregation(aggregationName, field);
    }

    private Aggregation getSumAggregation(String aggregationName, JSONObject aggregateJson) {
        String field = aggregateJson.getString("field");
        return new SumAggregation(aggregationName, field);
    }

    private Aggregation getAvgAggregation(String aggregationName, JSONObject aggregateJson) {
        String field = aggregateJson.getString("field");
        return new AvgAggregation(aggregationName, field);
    }

    private Aggregation getMinAggregation(String aggregationName, JSONObject aggregateJson) {
        String field = aggregateJson.getString("field");
        return new MinAggregation(aggregationName, field);
    }

    private Aggregation getMaxAggregation(String aggregationName, JSONObject aggregateJson) {
        String field = aggregateJson.getString("field");
        return new MaxAggregation(aggregationName, field);
    }

    private Aggregation getCalendarFieldAggregation(String aggregationName, JSONObject aggregateJson) {
        String field = aggregateJson.getString("field");
        Long minDocumentCount = JSONUtil.getOptionalLong(aggregateJson, "minDocumentCount");
        String timeZoneString = aggregateJson.optString("timeZone");
        TimeZone timeZone = timeZoneString == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZoneString);
        ChronoField calendarField = getChronoField(aggregateJson.getString("calendarField"));
        return new ChronoFieldAggregation(aggregationName, field, minDocumentCount, timeZone, calendarField);
    }

    private ChronoField getChronoField(String chronoField) {
        return ChronoField.valueOf(chronoField);
    }

    protected void applySortToQuery(GeQueryBuilder query, SearchOptions searchOptions) {
        String[] sorts = searchOptions.getOptionalParameter("sort[]", String[].class);
        if (sorts == null) {
            JSONArray sortsJson = searchOptions.getOptionalParameter("sort", JSONArray.class);
            if (sortsJson != null) {
                sorts = JSONUtil.toStringList(sortsJson).toArray(new String[sortsJson.length()]);
            }
        }
        if (sorts == null) {
            return;
        }
        for (String sort : sorts) {
            String propertyName = sort;
            SortDirection direction = SortDirection.ASCENDING;
            if (propertyName.toUpperCase().endsWith(":ASCENDING")) {
                propertyName = propertyName.substring(0, propertyName.length() - ":ASCENDING".length());
            } else if (propertyName.toUpperCase().endsWith(":DESCENDING")) {
                direction = SortDirection.DESCENDING;
                propertyName = propertyName.substring(0, propertyName.length() - ":DESCENDING".length());
            }
            query.sort(propertyName, direction);
        }
    }

    protected QueryResultsIterable<? extends GeObject> getSearchResults(Query query, FetchHints fetchHints) {
        //noinspection unused
        try (TraceSpan trace = Trace.start("getSearchResults")) {
            try (QueryResultsIterable<? extends GeObject> iter = query.search(getResultType(), fetchHints)) {
                return iter;
            } catch(IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    protected abstract EnumSet<GeObjectType> getResultType();

    protected abstract BoolQueryBuilder getQuery(SearchOptions searchOptions, Authorizations authorizations);

    protected void applyConceptTypeFilterToQuery(BoolQueryBuilder query, SearchOptions searchOptions) {
        Collection<SchemaRepository.ElementTypeFilter> conceptTypeFilters = getConceptTypeFilters(searchOptions);
        if (conceptTypeFilters != null) {
            schemaRepository.addConceptTypeFilterToQuery(query, conceptTypeFilters, searchOptions.getWorkspaceId());
        }
    }

    protected void applyEdgeLabelFilterToQuery(BoolQueryBuilder query, SearchOptions searchOptions) {
        Collection<SchemaRepository.ElementTypeFilter> edgeFilters = getEdgeLabelFilters(searchOptions);
        if (edgeFilters != null) {
            schemaRepository.addEdgeLabelFilterToQuery(query, edgeFilters, searchOptions.getWorkspaceId());
        }
    }

    protected Collection<SchemaRepository.ElementTypeFilter> getEdgeLabelFilters(SearchOptions searchOptions) {
        return getElementTypeFilters("edgeLabels", "edgeLabel", searchOptions);
    }

    protected Collection<SchemaRepository.ElementTypeFilter> getConceptTypeFilters(SearchOptions searchOptions) {
        return getElementTypeFilters("conceptTypes", "conceptType", searchOptions);
    }

    private Collection<SchemaRepository.ElementTypeFilter> getElementTypeFilters(String parameterName, String legacyParameterName, SearchOptions searchOptions) {
        String typesStr = searchOptions.getOptionalParameter(parameterName, String.class);
        if (typesStr != null) {
            JSONArray types = new JSONArray(typesStr);
            List<SchemaRepository.ElementTypeFilter> filters = new ArrayList<>(types.length());
            for (int i = 0; i < types.length(); i++) {
                JSONObject type = types.getJSONObject(i);
                SchemaRepository.ElementTypeFilter filter = ClientApiConverter.toClientApi(type, SchemaRepository.ElementTypeFilter.class);
                filters.add(filter);
            }
            return filters;
        }

        // Try the legacy parameter
        String elementType = searchOptions.getOptionalParameter(legacyParameterName, String.class);
        if (elementType != null) {
            Boolean includeChildNodes = searchOptions.getOptionalParameter("includeChildNodes", Boolean.class);
            if (includeChildNodes == null) {
                includeChildNodes = true;
            }
            return Collections.singleton(new SchemaRepository.ElementTypeFilter(elementType, includeChildNodes));
        }
        return null;
    }

    protected void applyFiltersToQuery(BoolQueryBuilder query, JSONArray filterJson, SearchOptions searchOptions) {
        String _logicalQuery = getLogicalOperators(searchOptions);
        if (!StringUtils.isEmpty(_logicalQuery)) {
            try {
                if (_logicalQuery.isEmpty()) {
                    _logicalQuery = "[]";
                }
                JSONArray logicalOperations = new JSONArray(_logicalQuery);
                if (logicalOperations.length() > 0) {
                    BoolQueryBuilder level0BoolQuery = boolQuery();
                    BoolQueryBuilder level1BoolQuery;
                    BoolQueryBuilder level2BoolQuery;
                    int counter = 0;
                    for (int i = 0; i < logicalOperations.length(); i++) {
                        JSONObject level0Object = (JSONObject) logicalOperations.get(i);
                        if (level0Object.has("children") && !level0Object.isNull("children")) {
                            level1BoolQuery = boolQuery();
                            JSONArray level1GroupArray = (JSONArray) level0Object.get("children");
                            for (int j = 0; j < level1GroupArray.length(); j++) {
                                JSONObject level1Object = (JSONObject) level1GroupArray.get(j);
                                String operatorLevel1 = "";
                                if (level1Object.has("operator") && !level1Object.isNull("operator")) {
                                    operatorLevel1 = level1Object.get("operator").toString();
                                }

                                //No operator, try to get from next or should clause
                                if (operatorLevel1.equals("")) {
                                    if ((j + 1) < level1GroupArray.length()) {
                                        JSONObject operatorObject = (JSONObject) level1GroupArray.get(j + 1);
                                        if (operatorObject.has("operator") && !operatorObject.isNull("operator")) {
                                            operatorLevel1 = operatorObject.get("operator").toString();
                                        }
                                    }
                                }

                                // Level2 Group
                                if (level1Object.has("children") && !level1Object.isNull("children")) {
                                    level2BoolQuery = boolQuery();
                                    JSONArray level2GroupArray = (JSONArray) level1Object.get("children");
                                    for (int k = 0; k < level2GroupArray.length(); k++) {
                                        JSONObject level2Object = (JSONObject) level2GroupArray.get(k);
                                        String operatorLevel2 = "";
                                        if (level2Object.has("operator") && !level2Object.isNull("operator")) {
                                            operatorLevel2 = level2Object.get("operator").toString();
                                        }

                                        //No operator, try to get from next or should clause
                                        if (operatorLevel2.equals("")) {
                                            if ((k + 1) < level2GroupArray.length()) {
                                                JSONObject operatorObject = (JSONObject) level2GroupArray.get(k + 1);
                                                if (operatorObject.has("operator") && !operatorObject.isNull("operator")) {
                                                    operatorLevel2 = operatorObject.get("operator").toString();
                                                }
                                            }
                                        }

                                        JSONObject obj = filterJson.getJSONObject(level2Object.getInt("id"));
                                        BoolQueryBuilder qb = getFilterForValueOrDataType(obj, filterJson, searchOptions);
                                        if (obj.length() > 0) {
                                            if (operatorLevel2.equalsIgnoreCase("and")) {
                                                level2BoolQuery.and(qb);
                                                counter++;
                                            } else if (operatorLevel2.equalsIgnoreCase("or")) {
                                                level2BoolQuery.or(qb);
                                                counter++;
                                            } else if (operatorLevel2.equalsIgnoreCase("not")) {
                                                level2BoolQuery.andNot(qb);
                                                counter++;
                                            } else {
                                                level2BoolQuery.or(qb);
                                                counter++;
                                            }
                                        }
                                    }
                                    if (operatorLevel1.equalsIgnoreCase("and")) {
                                        level1BoolQuery.and(level2BoolQuery);
                                    } else if (operatorLevel1.equalsIgnoreCase("or")) {
                                        level1BoolQuery.or(level2BoolQuery);
                                    } else if (operatorLevel1.equalsIgnoreCase("not")) {
                                        level1BoolQuery.andNot(level2BoolQuery);
                                    } else {
                                        level1BoolQuery.or(level2BoolQuery);
                                    }

                                } else {
                                    JSONObject obj = filterJson.getJSONObject(level1Object.getInt("id"));
                                    BoolQueryBuilder qb = getFilterForValueOrDataType(obj, filterJson, searchOptions);
                                    if (obj.length() > 0) {
                                        if (operatorLevel1.equalsIgnoreCase("and")) {
                                            level1BoolQuery.and(qb);
                                            counter++;
                                        } else if (operatorLevel1.equalsIgnoreCase("or")) {
                                            level1BoolQuery.or(qb);
                                            counter++;
                                        } else if (operatorLevel1.equalsIgnoreCase("not")) {
                                            level1BoolQuery.andNot(qb);
                                            counter++;
                                        } else {
                                            level1BoolQuery.or(qb);
                                            counter++;
                                        }
                                    }
                                }
                            }
                            //Add group operation
                            String operatorLevel0Group = "";
                            if (level0Object.has("operator")) {
                                operatorLevel0Group = level0Object.get("operator").toString();
                            }
                            //No operator, try to get from next or should clause
                            if (operatorLevel0Group.equals("")) {
                                if ((i + 1) < logicalOperations.length()) {
                                    JSONObject operatorObject = (JSONObject) logicalOperations.get(i + 1);
                                    if (operatorObject.has("operator") && !operatorObject.isNull("operator")) {
                                        operatorLevel0Group = operatorObject.get("operator").toString();
                                    }
                                }
                            }


                            if (operatorLevel0Group.equalsIgnoreCase("and")) {
                                level0BoolQuery.and(level1BoolQuery);
                            } else if (operatorLevel0Group.equalsIgnoreCase("or")) {
                                level0BoolQuery.or(level1BoolQuery);
                            } else if (operatorLevel0Group.equalsIgnoreCase("not")) {
                                level0BoolQuery.andNot(level1BoolQuery);
                            } else {
                                level0BoolQuery.or(level1BoolQuery);
                            }

                        } else {
                            String operatorLevel0 = "";
                            if (level0Object.has("operator") && !level0Object.isNull("operator")) {
                                operatorLevel0 = level0Object.get("operator").toString();
                            }

                            //No operator, try to get from next or should clause
                            if (operatorLevel0.equals("")) {
                                if ((i + 1) < logicalOperations.length()) {
                                    JSONObject operatorObject = (JSONObject) logicalOperations.get(i + 1);
                                    if (operatorObject.has("operator") && !operatorObject.isNull("operator")) {
                                        String nextOp = operatorObject.get("operator").toString();
                                        // if we are first operator in the chain, we can't take not from next op, just use and
                                        if (i == 0 && nextOp.equalsIgnoreCase("not")) {
                                            operatorLevel0 = "and";
                                        } else {
                                            operatorLevel0 = nextOp;
                                        }
                                    }
                                }
                            }

                            JSONObject obj = filterJson.getJSONObject(level0Object.getInt("id"));
                            BoolQueryBuilder qb = getFilterForValueOrDataType(obj, filterJson, searchOptions);
                            if (obj.length() > 0) {
                                if (operatorLevel0.equalsIgnoreCase("and")) {
                                    level0BoolQuery.and(qb);
                                    counter++;
                                } else if (operatorLevel0.equalsIgnoreCase("or")) {
                                    level0BoolQuery.or(qb);
                                    counter++;
                                } else if (operatorLevel0.equalsIgnoreCase("not")) {
                                    level0BoolQuery.andNot(qb);
                                    counter++;
                                } else {
                                    level0BoolQuery.or(qb);
                                    counter++;
                                }
                            }
                        }
                    }

                    if (filterJson.length() > 0) {
                        query.and(level0BoolQuery);
                        //Add remaining filters
                        if (counter < filterJson.length()) {
                            for (int i = counter; i < filterJson.length(); i++) {
                                JSONObject obj = filterJson.getJSONObject(i);
                                if (obj.length() > 0) {
                                    query.and(getFilterForValueOrDataType(obj, filterJson, searchOptions));
                                }
                            }
                        }
                    }
                }
            } catch (JSONException ex) {
                throw new BcException("Could not parse logical query string: "+ex.getMessage());
            }
        } else {
            BoolQueryBuilder queryBuilder = boolQuery();
            for (int i = 0; i < filterJson.length(); i++) {
                JSONObject obj = filterJson.getJSONObject(i);
                if (obj.length() > 0) {
                    queryBuilder.and(getFilterForValueOrDataType(obj, filterJson, searchOptions));
                }
            }
            query.and(queryBuilder);
        }
    }

    private BoolQueryBuilder getFilterForValueOrDataType(JSONObject obj, JSONArray filterJson, SearchOptions searchOptions) {
        if (obj.has("propertyName")) {
            return getPropertyNameFilter(obj, searchOptions);
        } else if (obj.has("dataType")) {
            return getDataTypeFilter(obj);
        } else {
            throw new BcException("Query filters must have either a propertyName or dataType field. Invalid filter: " + filterJson);
        }
    }

    private BoolQueryBuilder getPropertyNameFilter(JSONObject obj, SearchOptions searchOptions) {
        try {
            String predicateString = obj.optString("predicate");
            String propertyName = obj.getString("propertyName");
            if ("has".equals(predicateString)) {
                return boolQuery().and(GeQueryBuilders.exists(propertyName));
            } else if ("hasNot".equals(predicateString)) {
                return boolQuery().andNot(GeQueryBuilders.exists(propertyName));
            } else if ("in".equals(predicateString)) {
                JSONArray arr = obj.getJSONArray("values");
                Value valueArr = convertJsonArray(arr);
                return boolQuery().and(hasFilter(propertyName, Contains.IN, valueArr));
            } else {
                PropertyType propertyDataType = PropertyType.convert(obj.optString("propertyDataType"));
                JSONArray values = obj.getJSONArray("values");
                Object value0 = jsonValueToObject(values, propertyDataType, 0);

                if (PropertyType.STRING.equals(propertyDataType) && (predicateString == null || "~".equals(predicateString) || "".equals(predicateString))) {
                    return boolQuery().and(hasFilter(propertyName, TextPredicate.CONTAINS, (Value) value0));
                } else if (PropertyType.DATETIME.equals(propertyDataType) || PropertyType.DATE.equals(propertyDataType) || PropertyType.LOCAL_DATE.equals(propertyDataType) || PropertyType.LOCAL_DATETIME.equals(propertyDataType)) {
                    return applyDateToQuery(obj, predicateString, values, searchOptions, propertyDataType);
                } else if (PropertyType.BOOLEAN.equals(propertyDataType)) {
                    return boolQuery().and(hasFilter(propertyName, Compare.EQUAL, (Value) value0));
                } else if (PropertyType.GEO_LOCATION.equals(propertyDataType)) {
                    GeoCompare geoComparePredicate = GeoCompare.valueOf(predicateString.toUpperCase());
                    return boolQuery().and(hasFilter(propertyName, geoComparePredicate, (Value) value0));
                } else if ("<".equals(predicateString)) {
                    return boolQuery().and(hasFilter(propertyName, Compare.LESS_THAN, (Value) value0));
                } else if ("<=".equals(predicateString)) {
                    return boolQuery().and(hasFilter(propertyName, Compare.LESS_THAN_EQUAL, (Value) value0));
                } else if (">".equals(predicateString)) {
                    return boolQuery().and(hasFilter(propertyName, Compare.GREATER_THAN, (Value) value0));
                } else if (">=".equals(predicateString)) {
                    return boolQuery().and(hasFilter(propertyName, Compare.GREATER_THAN_EQUAL, (Value) value0));
                } else if ("range".equals(predicateString)) {
                    Object value1 = jsonValueToObject(values, propertyDataType, 1);
                    if (value0 instanceof DateTimeValue && value1 instanceof DateTimeValue) {
                        ZonedDateTime start = ((DateTimeValue) value0).asObjectCopy();
                        ZonedDateTime end = ((DateTimeValue) value1).asObjectCopy();
                        return boolQuery().and(hasFilter(propertyName, Compare.RANGE,
                                Values.of(new Range<>(start, true, end, false))));
                    } else {
                        throw new IllegalArgumentException("Range query must have DateTimeValue values");
                    }
                } else if ("=".equals(predicateString) || "equal".equals(predicateString)) {
                    if (PropertyType.DOUBLE.equals(propertyDataType)) {
                        return applyDoubleEqualityToQuery(obj, value0);
                    } else {
                        return boolQuery().and(hasFilter(propertyName, Compare.EQUAL, (Value) value0));
                    }
                } else {
                    throw new BcException("unhandled query\n" + obj.toString(2));
                }
            }
        } catch (ParseException ex) {
            throw new BcException("Could not update query with filter:\n" + obj.toString(2), ex);
        }
    }

    private BoolQueryBuilder getDataTypeFilter(JSONObject obj) {
        String dataType = obj.getString("dataType");
        String predicateString = obj.optString("predicate");
        PropertyType propertyType = PropertyType.valueOf(dataType);

        try {
            if ("has".equals(predicateString)) {
                return boolQuery().and(exists(graph, PropertyType.getTypeClass(propertyType)));
            } else if ("hasNot".equals(predicateString)) {
                return boolQuery().andNot(exists(graph, PropertyType.getTypeClass(propertyType)));
            } else if ("in".equals(predicateString)) {
                JSONArray values = obj.getJSONArray("values");
                Value valueArr = convertJsonArray(values);
                return boolQuery().and(hasFilter(graph, PropertyType.getTypeClass(propertyType), Contains.IN, Values.of(valueArr)));
            } else {
                JSONArray values = obj.getJSONArray("values");
                Object value0 = jsonValueToObject(values, propertyType, 0);
                if (PropertyType.GEO_LOCATION.equals(propertyType)) {
                    GeoCompare geoComparePredicate = GeoCompare.valueOf(predicateString.toUpperCase());
                    return boolQuery().and(hasFilter(graph, GeoShapeValue.class, geoComparePredicate, (Value) value0));
                } else {
                    throw new UnsupportedOperationException("Data type queries are not yet supported for type: " + dataType);
                }
            }
        } catch (ParseException ex) {
            throw new BcException("Could not update query with filter:\n" + obj.toString(2), ex);
        }
    }

    protected JSONArray getFilterJson(SearchOptions searchOptions, String workspaceId) {
        JSONArray filterJson = searchOptions.getRequiredParameter("filter", JSONArray.class);
        schemaRepository.resolvePropertyIds(filterJson, workspaceId);
        return filterJson;
    }

    protected JSONArray getSourceFilterJson(SearchOptions searchOptions) {
        JSONArray filterJson = searchOptions.getOptionalParameter(SearchSchema.SOURCE_SEARCH_KEY, JSONArray.class);
        if (filterJson != null) {
            schemaRepository.resolvePropertyIds(filterJson);
        }
        return filterJson;
    }

    protected String getLogicalOperators(SearchOptions searchOptions) {
        return searchOptions.getOptionalParameter("logicalSourceString", String.class);
    }

    protected void applyRefinementsToQuery(BoolQueryBuilder query, SearchOptions searchOptions) {
        JSONArray refinementJson = searchOptions.getOptionalParameter("refinement", JSONArray.class);
        if(refinementJson == null)
            return;

        for (int i = 0; i < refinementJson.length(); i++) {
            JSONObject obj = refinementJson.getJSONObject(i);
            if (obj.length() > 0) {
                String propertyName = obj.getString("field");
                String refinementType = obj.getString("type");

                if ("conceptType".equals(propertyName) || SearchIndex.CONCEPT_TYPE_FIELD_NAME.equals(propertyName)) {
                    String propertyValue = obj.getString("bucketKey");
                    query.and(hasConceptType(propertyValue));
                } else {
                    SchemaProperty property = schemaRepository.getPropertyByName(propertyName);
                    PropertyType propertyDataType = property.getDataType();

                    if ("term".equals(refinementType)) {
                        String propertyValue = obj.getString("bucketKey");
                        if (EMPTY_REFINEMENT_STRING.equals(propertyValue)) {
                            query.andNot(exists(propertyName));
                        } else {
                            switch (propertyDataType) {
                                case DATETIME:
                                    DateTimeValue dtv = DateTimeValue.ofEpochMillis(longValue(Long.parseLong(propertyValue)));
                                    String displayType = property.getDisplayType();
                                    boolean isDateOnly = displayType != null && displayType.equals("dateOnly");
                                    dtv = moveDateToStart(dtv);
                                    query.and(hasFilter(propertyName, Compare.GREATER_THAN_EQUAL, dtv));
                                    dtv = moveDateToEnd(dtv, isDateOnly);
                                    query.and(hasFilter(propertyName, Compare.LESS_THAN, dtv));
                                    break;
                                case BOOLEAN:
                                    query.and(hasFilter(propertyName, Compare.EQUAL, booleanValue(Boolean.parseBoolean(propertyValue))));
                                    break;
                                case DOUBLE:
                                    query.and(hasFilter(propertyName, Compare.EQUAL, doubleValue(Double.parseDouble(propertyValue))));
                                    break;
                                case INTEGER:
                                    query.and(hasFilter(propertyName, Compare.EQUAL, intValue(Integer.parseInt(propertyValue))));
                                    break;
                                default:
                                    query.and(hasFilter(propertyName, Compare.EQUAL, Values.of(propertyValue)));
                            }
                        }
                    } else if ("histogram".equals(refinementType)) {
                        String fromValue = null;
                        String toValue = null;

                        if (obj.has("bucketFromValue"))
                            fromValue = obj.getString("bucketFromValue");

                        if (obj.has("bucketToValue"))
                            toValue = obj.getString("bucketToValue");

                        switch (propertyDataType) {
                            case DATETIME:
                                String displayType = property.getDisplayType();
                                boolean isDateOnly = displayType != null && displayType.equals("dateOnly");

                                if (fromValue != null) {
                                    DateTimeValue dtv = DateTimeValue.ofEpochMillis(longValue(Long.parseLong(fromValue)));
                                    dtv = moveDateToStart(dtv);
                                    query.and(hasFilter(propertyName, Compare.GREATER_THAN, dtv));
                                }

                                if (toValue != null) {
                                    DateTimeValue dtv = DateTimeValue.ofEpochMillis(longValue(Long.parseLong(toValue)));
                                    dtv = moveDateToEnd(dtv, isDateOnly);
                                    query.and(hasFilter(propertyName, Compare.LESS_THAN_EQUAL, dtv));
                                }
                                break;
                            case DOUBLE:
                            case INTEGER:
                                if (fromValue != null) {
                                    query.and(hasFilter(propertyName, Compare.GREATER_THAN, doubleValue(Double.parseDouble(fromValue))));
                                }

                                if (toValue != null) {
                                    query.and(hasFilter(propertyName, Compare.LESS_THAN_EQUAL, doubleValue(Double.parseDouble(toValue))));
                                }
                                break;
                            default:
                                throw new BcException("unhandled histogram type\n" + obj.toString(2));
                        }
                    } else {
                        throw new BcException("unhandled refinement type\n" + obj.toString(2));
                    }
                }
            }
        }
    }

    private BoolQueryBuilder applyDoubleEqualityToQuery(JSONObject obj, Object value0) throws ParseException {
        String propertyName = obj.getString("propertyName");
        JSONObject metadata = obj.has("metadata") ? obj.getJSONObject("metadata") : null;

        if (metadata != null && metadata.has("inputPrecision") && value0 instanceof Double) {
            double doubleParam = (double) value0;
            int inputPrecision = Math.max(metadata.getInt("inputPrecision"), 0);
            double lowerBound = Precision.round(doubleParam, inputPrecision, BigDecimal.ROUND_DOWN);
            double upperBound = Precision.equals(doubleParam, lowerBound, Precision.EPSILON) ? lowerBound + Math.pow(10, -inputPrecision) :
                    Precision.round(doubleParam, inputPrecision, BigDecimal.ROUND_UP);

            return boolQuery()
                    .and(hasFilter(propertyName, Compare.GREATER_THAN_EQUAL, doubleValue(lowerBound - Precision.EPSILON)))
                    .and(hasFilter(propertyName, Compare.LESS_THAN, doubleValue(upperBound + Precision.EPSILON)));
        } else {
            return boolQuery().and(hasFilter(propertyName, Compare.EQUAL, Values.of(value0)));
        }
    }

    private BoolQueryBuilder applyDateToQuery(JSONObject obj, String predicate, JSONArray values, SearchOptions searchOptions, PropertyType type) throws ParseException {
        String propertyName = obj.getString("propertyName");
        SchemaProperty property = schemaRepository.getPropertyByName(propertyName, searchOptions.getWorkspaceId());

        if (property != null && values.length() > 0) {
            if (predicate == null || predicate.equals("equal") || predicate.equals("=")) {
                TemporalValue<?, ?> value = convertToTemporalValue(values, 0, type);
                return boolQuery().and(hasFilter(propertyName, Compare.GREATER_THAN_EQUAL, value))
                        .and(hasFilter(propertyName, Compare.LESS_THAN_EQUAL, value));
            } else if (predicate.equals("range")) {
                if (values.length() > 1) {
                    TemporalValue<?, ?> startDate = convertToTemporalValue(values, 0, type);
                    TemporalValue<?, ?> endDate = convertToTemporalValue(values, 1, type);
                    int compare = COMPARATOR.compare(startDate, endDate);
                    if (compare < 0) {
                        return boolQuery().and(hasFilter(propertyName, Compare.RANGE,
                                Values.of(new Range<>(startDate.asObjectCopy(), true, endDate.asObjectCopy(), false))));
                    } else {
                        return boolQuery().and(hasFilter(propertyName, Compare.RANGE,
                                Values.of(new Range<>(endDate.asObjectCopy(), true, startDate.asObjectCopy(), false))));
                    }
                }
            } else if (predicate.equals("<")) {
                TemporalValue<?, ?> value = convertToTemporalValue(values, 0, type);
                return boolQuery().and(hasFilter(propertyName, Compare.LESS_THAN_EQUAL, value));
            } else if (predicate.equals(">")) {
                TemporalValue<?, ?> value = convertToTemporalValue(values, 0, type);
                return boolQuery().and(hasFilter(propertyName, Compare.GREATER_THAN_EQUAL, value));
            }
        }

        throw new BcException("Could not find property with name: "+propertyName);
    }

    TemporalValue<?, ?> convertToTemporalValue(JSONArray values, int index, PropertyType type) throws ParseException {
        TemporalValue<?,?> calendar;
        boolean isRelative = values.get(index) instanceof JSONObject;
        if (isRelative) {
            JSONObject fromNow = (JSONObject) values.get(index);
            switch (type) {
                case DATE:
                case DATETIME:
                    calendar = DateTimeValue.now(Clocks.systemClock());
                    break;
                case LOCAL_DATETIME:
                    calendar = LocalDateTimeValue.now(Clocks.systemClock());
                    break;
                case LOCAL_DATE:
                    calendar = DateValue.now(Clocks.systemClock());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid date type passed: "+type);
            }

            calendar = moveDate(calendar, fromNow.getInt("unit"), fromNow.getInt("amount"));
        } else {
            calendar = (TemporalValue<?, ?>) jsonValueToObject(values, type, index);
        }
        return calendar;
    }

    private TemporalValue<?, ?> moveDate(TemporalValue<?, ?> date, int calendarField, int amount) {
        switch (calendarField) {
            case Calendar.YEAR:
                return date.plus(amount, ChronoUnit.YEARS);
            case Calendar.MONTH:
                return date.plus(amount, ChronoUnit.MONTHS);
            case Calendar.WEEK_OF_YEAR:
                return date.plus(amount, ChronoUnit.WEEKS);
            case Calendar.DAY_OF_MONTH:
            case Calendar.DAY_OF_YEAR:
                return date.plus(amount, ChronoUnit.DAYS);
            case Calendar.HOUR_OF_DAY:
            case Calendar.HOUR:
                return date.plus(amount, ChronoUnit.HOURS);
            case Calendar.MINUTE:
                return date.plus(amount, ChronoUnit.MINUTES);
            case Calendar.SECOND:
                return date.plus(amount, ChronoUnit.SECONDS);
            case Calendar.MILLISECOND:
                return date.plus(amount, ChronoUnit.MILLIS);
            default:
                throw new IllegalArgumentException("Unknown calendar field: "+calendarField);
        }
    }

    private DateTimeValue moveDateToStart(DateTimeValue dtv) {
        ZonedDateTime zdt = dtv.asObjectCopy();
        return datetime(zdt.truncatedTo(ChronoUnit.DAYS));
    }

    private DateTimeValue moveDateToEnd(DateTimeValue dtv, boolean dateOnly) {
        if (dateOnly) {
            return dtv.plus(1, ChronoUnit.DAYS);
        } else {
            return dtv.plus(1, ChronoUnit.MINUTES);
        }
    }

    private Object jsonValueToObject(JSONArray values, PropertyType propertyDataType, int index) throws ParseException {
        // JSONObject can be sent to search in the case of relative date searching
        if (values.get(index) instanceof JSONObject) {
            return values.get(index);
        }
        return SchemaProperty.convert(values, propertyDataType, index);
    }

    protected Graph getGraph() {
        return graph;
    }

    public SchemaRepository getSchemaRepository() {
        return schemaRepository;
    }

    private Value convertJsonArray(JSONArray arr) {
        Object[] valueArr = new Object[arr.length()];
        for (int i = 0; i < arr.length(); i++) {
            valueArr[i] = arr.get(i);
        }
        if (valueArr.length > 0) {
            Class valueType = valueArr[0].getClass();
            Object[] newArr = (Object[]) Array.newInstance(valueType, valueArr.length);
            System.arraycopy(valueArr, 0, newArr, 0, valueArr.length);
            return Values.of(newArr);
        }
        return Values.stringArray();
    }
}
