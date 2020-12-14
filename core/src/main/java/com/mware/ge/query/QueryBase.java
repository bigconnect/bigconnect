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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mware.ge.*;
import com.mware.ge.query.aggregations.Aggregation;
import com.mware.ge.query.aggregations.AggregationResult;
import com.mware.ge.scoring.ScoringStrategy;
import com.mware.ge.sorting.SortingStrategy;
import com.mware.ge.type.GeoShape;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.SelectManyIterable;
import com.mware.ge.util.StreamUtils;
import com.mware.ge.values.storable.GeoShapeValue;
import com.mware.ge.values.storable.TemporalValue;
import com.mware.ge.values.storable.Value;
import org.apache.poi.ss.formula.functions.T;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class QueryBase implements Query, SimilarToGraphQuery {
    private final Graph graph;
    private final QueryParameters parameters;
    private String logicalQuery;
    private List<Aggregation> aggregations = new ArrayList<>();

    protected QueryBase(Graph graph, String queryString, String logicalQuery, Authorizations authorizations) {
        this.graph = graph;
        this.parameters = new QueryStringQueryParameters(queryString, authorizations);
        this.logicalQuery = logicalQuery;
    }

    protected QueryBase(Graph graph, String queryString, Authorizations authorizations) {
        this.graph = graph;
        this.parameters = new QueryStringQueryParameters(queryString, authorizations);
    }

    protected QueryBase(Graph graph, String[] similarToFields, String similarToText, Authorizations authorizations) {
        this.graph = graph;
        this.parameters = new SimilarToTextQueryParameters(similarToFields, similarToText, authorizations);
    }

    public String getLogicalQuery() {
        return this.logicalQuery;
    }

    public void setLogicalQuery(String logicalQuery) {
        this.logicalQuery = logicalQuery;
    }

    @Override
    public QueryResultsIterable<Vertex> vertices() {
        return vertices(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<Vertex>) search(EnumSet.of(GeObjectType.VERTEX), fetchHints);
    }

    @Override
    public QueryResultsIterable<String> vertexIds() {
        return vertexIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResultsIterable<String> vertexIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new DefaultGraphQueryIdIterable<>(vertices(fetchHints));
    }

    @Override
    public QueryResultsIterable<Edge> edges() {
        return edges(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<Edge> edges(final FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<Edge>) search(EnumSet.of(GeObjectType.EDGE), fetchHints);
    }

    @Override
    public QueryResultsIterable<String> edgeIds() {
        return edgeIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResultsIterable<String> edgeIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new DefaultGraphQueryIdIterable<>(edges(fetchHints));
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows() {
        return extendedDataRows(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<ExtendedDataRow>) search(EnumSet.of(GeObjectType.EXTENDED_DATA), fetchHints);
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds() {
        return extendedDataRowIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        QueryResultsIterable<? extends GeObject> geObjects = search(EnumSet.of(GeObjectType.EXTENDED_DATA), fetchHints);
        return new DefaultGraphQueryIdIterable<>(geObjects);
    }

    @Override
    public QueryResultsIterable<? extends GeObject> search() {
        return search(GeObjectType.ALL, getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<? extends GeObject> search(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints) {
        List<QueryResultsIterable<? extends GeObject>> items = new ArrayList<>();
        if (objectTypes.contains(GeObjectType.VERTEX)) {
            items.add(vertices(fetchHints));
        }
        if (objectTypes.contains(GeObjectType.EDGE)) {
            items.add(edges(fetchHints));
        }
        if (objectTypes.contains(GeObjectType.EXTENDED_DATA)) {
            items.add(extendedData(fetchHints));
        }

        if (items.size() == 1) {
            return items.get(0);
        }

        return new SelectManySearch(items);
    }

    private static class SelectManySearch
            extends SelectManyIterable<QueryResultsIterable<? extends GeObject>, GeObject>
            implements QueryResultsIterable<GeObject> {
        public SelectManySearch(Iterable<? extends QueryResultsIterable<? extends GeObject>> source) {
            super(source);
        }

        @Override
        public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
            throw new GeException("Not implemented");
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public long getTotalHits() {
            long totalHits = 0;
            for (QueryResultsIterable queryResultsIterable : getSource()) {
                totalHits += queryResultsIterable.getTotalHits();
            }
            return totalHits;
        }

        @Override
        protected Iterable<? extends GeObject> getIterable(QueryResultsIterable<? extends GeObject> source) {
            return source;
        }
    }

    /**
     * This method should be overridden if {@link #search(EnumSet, FetchHints)} is not overridden.
     */
    protected QueryResultsIterable<? extends GeObject> extendedData(FetchHints fetchHints) {
        throw new GeException("not implemented");
    }

    protected QueryResultsIterable<? extends GeObject> extendedData(FetchHints fetchHints, Iterable<? extends Element> elements) {
        Iterable<ExtendedDataRow> allExtendedData = new SelectManyIterable<Element, ExtendedDataRow>(elements) {
            @Override
            protected Iterable<? extends ExtendedDataRow> getIterable(Element element) {
                return new SelectManyIterable<String, ExtendedDataRow>(element.getExtendedDataTableNames()) {
                    @Override
                    protected Iterable<? extends ExtendedDataRow> getIterable(String tableName) {
                        return element.getExtendedData(tableName, fetchHints);
                    }
                };
            }
        };
        return new DefaultGraphQueryIterableWithAggregations<>(getParameters(), allExtendedData, true, true, true, getAggregations());
    }

    @Override
    public Query hasId(String... ids) {
        getParameters().addIds(Arrays.asList(ids));
        return this;
    }

    @Override
    public Query hasId(Iterable<String> ids) {
        getParameters().addIds(IterableUtils.toList(ids));
        return this;
    }

    @Override
    public Query hasConceptType(String... conceptTypes) {
        for (String conceptType : conceptTypes) {
            getParameters().addConceptType(conceptType);
        }
        return this;
    }

    @Override
    public Query hasConceptType(Collection<String> conceptTypes) {
        for (String conceptType : conceptTypes) {
            getParameters().addConceptType(conceptType);
        }
        return this;
    }

    @Override
    public Query hasEdgeLabel(String... edgeLabels) {
        for (String edgeLabel : edgeLabels) {
            getParameters().addEdgeLabel(edgeLabel);
        }
        return this;
    }

    @Override
    public Query hasEdgeLabel(Collection<String> edgeLabels) {
        for (String edgeLabel : edgeLabels) {
            getParameters().addEdgeLabel(edgeLabel);
        }
        return this;
    }

    public Query hasInVertexTypes(String... vertexTypes) {
        for (String vertexType : vertexTypes) {
            getParameters().addInVertexType(vertexType);
        }
        return this;
    }

    public Query hasOutVertexTypes(String... vertexTypes) {
        for (String vertexType : vertexTypes) {
            getParameters().addOutVertexType(vertexType);
        }
        return this;
    }

    @Override
    public Query hasAuthorization(String... authorizations) {
        getParameters().addHasContainer(new HasAuthorizationContainer(Conjunction.AND, Arrays.asList(authorizations)));
        return this;
    }
    @Override
    public Query hasAuthorization(Iterable<String> authorizations) {
        getParameters().addHasContainer(new HasAuthorizationContainer(Conjunction.AND, authorizations));
        return this;
    }

    @Override
    public Query hasExtendedData(ElementType elementType, String elementId) {
        return hasExtendedData(elementType, elementId, null);
    }

    @Override
    public Query hasExtendedData(String tableName) {
        return hasExtendedData(null, null, tableName);
    }

    @Override
    public Query hasExtendedData(ElementType elementType, String elementId, String tableName) {
        hasExtendedData(Lists.newArrayList(new HasExtendedDataFilter(elementType, elementId, tableName)));
        return this;
    }

    @Override
    public Query hasExtendedData(Iterable<HasExtendedDataFilter> filters) {
        getParameters().addHasContainer(new HasExtendedData(Conjunction.AND, ImmutableList.copyOf(filters)));
        return this;
    }

    @Override
    @Deprecated
    public QueryResultsIterable<Edge> edges(final String label, FetchHints fetchHints) {
        hasEdgeLabel(label);
        return edges(fetchHints);
    }

    @Override
    @Deprecated
    public QueryResultsIterable<Edge> edges(final String label) {
        hasEdgeLabel(label);
        return edges();
    }

    @Override
    public QueryResultsIterable<Element> elements() {
        return elements(getGraph().getDefaultFetchHints());
    }

    @Override
    public QueryResultsIterable<Element> elements(FetchHints fetchHints) {
        //noinspection unchecked
        return (QueryResultsIterable<Element>) search(GeObjectType.ELEMENTS, fetchHints);
    }

    @Override
    public QueryResultsIterable<String> elementIds() {
        return elementIds(IdFetchHint.NONE);
    }

    @Override
    public QueryResultsIterable<String> elementIds(EnumSet<IdFetchHint> idFetchHints) {
        FetchHints fetchHints = idFetchHintsToElementFetchHints(idFetchHints);
        return new DefaultGraphQueryIdIterable<>(elements(fetchHints));
    }

    @Override
    public <T extends Value> Query range(String propertyName, T startValue, T endValue) {
        return range(propertyName, startValue, true, endValue, true);
    }

    @Override
    public <T extends Value> Query range(String propertyName, T startValue, boolean inclusiveStartValue, T endValue, boolean inclusiveEndValue) {
        if (startValue != null) {
            this.parameters.addHasContainer(new HasValueContainer(Conjunction.AND, propertyName, inclusiveStartValue ? Compare.GREATER_THAN_EQUAL : Compare.GREATER_THAN, startValue, getGraph().getPropertyDefinitions()));
        }
        if (endValue != null) {
            this.parameters.addHasContainer(new HasValueContainer(Conjunction.AND, propertyName, inclusiveEndValue ? Compare.LESS_THAN_EQUAL : Compare.LESS_THAN, endValue, getGraph().getPropertyDefinitions()));
        }
        return this;
    }

    @Override
    public Query sort(String propertyName, SortDirection direction) {
        this.parameters.addSortContainer(new PropertySortContainer(propertyName, direction));
        return this;
    }

    @Override
    public Query sort(SortingStrategy sortingStrategy, SortDirection direction) {
        this.parameters.addSortContainer(new SortingStrategySortContainer(sortingStrategy, direction));
        return this;
    }

    @Override
    public <T extends Value> Query has(String propertyName, T value) {
        this.parameters.addHasContainer(new HasValueContainer(Conjunction.AND, propertyName, Compare.EQUAL, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T extends Value> Query hasNot(String propertyName, T value) {
        this.parameters.addHasContainer(new HasValueContainer(Conjunction.AND, propertyName, Contains.NOT_IN, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T extends Value> Query has(String propertyName, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasValueContainer(Conjunction.AND, propertyName, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T extends Value> Query has(String propertyName, Predicate predicate, Conjunction conjunction, T value) {
        this.parameters.addHasContainer(new HasValueContainer(conjunction, propertyName, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T extends Value, K extends Value> Query has(Class<T> dataType, Predicate predicate, K value) {
        this.parameters.addHasContainer(new HasValueContainer(Conjunction.AND, dataType, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T extends Value> Query has(Class<T> dataType) {
        this.parameters.addHasContainer(new HasPropertyContainer(Conjunction.AND, dataType, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T extends Value> Query hasNot(Class<T> dataType) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(Conjunction.AND, dataType, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public <T extends Value> Query has(Iterable<String> propertyNames, Predicate predicate, T value) {
        this.parameters.addHasContainer(new HasValueContainer(Conjunction.AND, propertyNames, predicate, value, getGraph().getPropertyDefinitions()));
        return this;
    }

    @Override
    public Query has(String propertyName) {
        this.parameters.addHasContainer(new HasPropertyContainer(Conjunction.AND, propertyName));
        return this;
    }

    @Override
    public <T> Query has(Iterable<String> propertyNames) {
        this.parameters.addHasContainer(new HasPropertyContainer(Conjunction.AND, propertyNames));
        return this;
    }

    @Override
    public Query hasNot(String propertyName) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(Conjunction.AND, propertyName));
        return this;
    }

    @Override
    public <T> Query hasNot(Iterable<String> propertyNames) {
        this.parameters.addHasContainer(new HasNotPropertyContainer(Conjunction.AND, propertyNames));
        return this;
    }

    @Override
    public Query skip(int count) {
        this.parameters.setSkip(count);
        return this;
    }

    @Override
    public Query limit(Integer count) {
        this.parameters.setLimit(count);
        return this;
    }

    @Override
    public Query limit(Long count) {
        this.parameters.setLimit(count);
        return this;
    }

    @Override
    public Query minScore(double score) {
        this.parameters.setMinScore(score);
        return this;
    }

    @Override
    public Query scoringStrategy(ScoringStrategy scoringStrategy) {
        this.parameters.setScoringStrategy(scoringStrategy);
        return this;
    }

    public Graph getGraph() {
        return graph;
    }

    public QueryParameters getParameters() {
        return parameters;
    }

    public static abstract class HasContainer {
        public final Conjunction conjunction;

        public HasContainer(Conjunction conjunction) {
            this.conjunction = conjunction;
        }

        public abstract boolean isMatch(GeObject elem);

        @Override
        public String toString() {
            return this.getClass().getName() + "{}";
        }

        @SuppressWarnings("unchecked")
        protected boolean isPropertyOfType(PropertyDefinition propertyDefinition, Class<? extends Value> dataType) {
            boolean propertyIsDate = TemporalValue.class.isAssignableFrom(propertyDefinition.getDataType());
            boolean dataTypeIsDate = TemporalValue.class.isAssignableFrom(dataType);

            return dataType.isAssignableFrom(propertyDefinition.getDataType()) || (propertyIsDate && dataTypeIsDate);
        }
    }

    public interface SortContainer {
    }

    public static class PropertySortContainer implements SortContainer {
        public final String propertyName;
        public final SortDirection direction;

        public PropertySortContainer(String propertyName, SortDirection direction) {
            this.propertyName = propertyName;
            this.direction = direction;
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    "propertyName='" + propertyName + '\'' +
                    ", direction=" + direction +
                    '}';
        }
    }

    public static class SortingStrategySortContainer implements SortContainer {
        public final SortingStrategy sortingStrategy;
        public final SortDirection direction;

        public SortingStrategySortContainer(SortingStrategy sortingStrategy, SortDirection direction) {
            this.sortingStrategy = sortingStrategy;
            this.direction = direction;
        }

        @Override
        public String toString() {
            return "SortingStrategySortContainer{" +
                    "sortingStrategy=" + sortingStrategy +
                    ", direction=" + direction +
                    '}';
        }
    }

    public static class HasAuthorizationContainer extends HasContainer {
        public final Set<String> authorizations;

        public HasAuthorizationContainer(Conjunction conjunction, Iterable<String> authorizations) {
            super(conjunction);
            this.authorizations = IterableUtils.toSet(authorizations);
        }

        @Override
        public boolean isMatch(GeObject geObject) {
            for (String authorization : authorizations) {
                if (geObject instanceof Element) {
                    Element element = (Element) geObject;

                    if (element.getVisibility().hasAuthorization(authorization)) {
                        return true;
                    }

                    boolean hiddenVisibilityMatches = StreamUtils.stream(element.getHiddenVisibilities())
                            .anyMatch(visibility -> visibility.hasAuthorization(authorization));
                    if (hiddenVisibilityMatches) {
                        return true;
                    }
                }

                boolean propertyMatches = StreamUtils.stream(geObject.getProperties())
                        .anyMatch(property -> {
                            if (property.getVisibility().hasAuthorization(authorization)) {
                                return true;
                            }
                            return StreamUtils.stream(property.getHiddenVisibilities())
                                    .anyMatch(visibility -> visibility.hasAuthorization(authorization));
                        });
                if (propertyMatches) {
                    return true;
                }
            }
            return false;
        }

        public Iterable<String> getAuthorizations() {
            return authorizations;
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    ", authorizations='" + Joiner.on(", ").join(authorizations) + '\'' +
                    '}';
        }
    }

    public static class HasValueContainer extends HasContainer {
        public final Set<String> keys;
        public final Value value;
        public final Predicate predicate;
        private final Collection<PropertyDefinition> propertyDefinitions;

        public HasValueContainer(Conjunction conjunction, String key, Predicate predicate, Value value, Collection<PropertyDefinition> propertyDefinitions) {
            this(conjunction, Collections.singleton(key), predicate, value, propertyDefinitions);
        }

        public HasValueContainer(Conjunction conjunction, Iterable<String> keys, Predicate predicate, Value value, Collection<PropertyDefinition> propertyDefinitions) {
            super(conjunction);
            this.keys = IterableUtils.toSet(keys);
            this.value = value;
            this.predicate = predicate;
            this.propertyDefinitions = propertyDefinitions;

            if (this.keys.isEmpty()) {
                throw new GeException("Invalid query parameters, no property names specified");
            }
            validateParameters();
        }

        public HasValueContainer(Conjunction conjunction, Class<? extends Value> dataType, Predicate predicate, Value value, Collection<PropertyDefinition> propertyDefinitions) {
            super(conjunction);
            this.value = value;
            this.predicate = predicate;
            this.keys = propertyDefinitions.stream()
                    .filter(propertyDefinition -> isPropertyOfType(propertyDefinition, dataType))
                    .map(PropertyDefinition::getPropertyName)
                    .collect(Collectors.toSet());
            this.propertyDefinitions = propertyDefinitions;

            if (this.keys.isEmpty()) {
                throw new GeException("Invalid query parameters, no properties of type " + dataType.getName() + " found");
            }
            validateParameters();
        }

        private void validateParameters() {
            this.keys.forEach(key -> {
                PropertyDefinition propertyDefinition = PropertyDefinition.findPropertyDefinition(propertyDefinitions, key);
                if (predicate instanceof TextPredicate && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.FULL_TEXT)) {
                    throw new GeException("Check your TextIndexHint settings. Property " + propertyDefinition.getPropertyName() + " is not full text indexed.");
                } else if (predicate instanceof GeoCompare && !isPropertyOfType(propertyDefinition, GeoShapeValue.class)) {
                    throw new GeException("GeoCompare query is only allowed for GeoShape types. Property " + propertyDefinition.getPropertyName() + " is not a GeoShape.");
                } else if (Compare.STARTS_WITH.equals(predicate) && !propertyDefinition.getTextIndexHints().contains(TextIndexHint.EXACT_MATCH)) {
                    throw new GeException("Check your TextIndexHint settings. Property " + propertyDefinition.getPropertyName() + " is not exact match indexed.");
                }
            });
        }

        @Override
        public boolean isMatch(GeObject geObject) {
            for (String key : this.keys) {
                if (this.predicate.evaluate(geObject.getProperties(key), this.value, this.propertyDefinitions)) {
                    return true;
                }
            }
            return false;
        }

        public Iterable<String> getKeys() {
            return ImmutableSet.copyOf(this.keys);
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    "predicate=" + predicate +
                    ", value=" + value +
                    ", keys='" + Joiner.on(", ").join(keys) + '\'' +
                    '}';
        }
    }

    public static class HasExtendedData extends HasContainer {
        private final ImmutableList<HasExtendedDataFilter> filters;

        public HasExtendedData(Conjunction conjunction, ImmutableList<HasExtendedDataFilter> filters) {
            super(conjunction);
            this.filters = filters;
        }

        public ImmutableList<HasExtendedDataFilter> getFilters() {
            return filters;
        }

        @Override
        public boolean isMatch(GeObject elem) {
            if (!(elem instanceof ExtendedDataRow)) {
                return false;
            }

            ExtendedDataRow row = (ExtendedDataRow) elem;
            ExtendedDataRowId rowId = row.getId();
            for (HasExtendedDataFilter filter : filters) {
                if (filter.getElementType() == null || rowId.getElementType().equals(filter.getElementType())
                        && (filter.getElementId() == null || rowId.getElementId().equals(filter.getElementId()))
                        && (filter.getTableName() == null || rowId.getTableName().equals(filter.getTableName()))) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class HasPropertyContainer extends HasContainer {
        private Set<String> keys;

        public HasPropertyContainer(Conjunction conjunction, String key) {
            super(conjunction);
            this.keys = Collections.singleton(key);
        }

        public HasPropertyContainer(Conjunction conjunction, Iterable<String> keys) {
            super(conjunction);
            this.keys = IterableUtils.toSet(keys);
        }

        public HasPropertyContainer(Conjunction conjunction, Class<? extends Value> dataType, Collection<PropertyDefinition> propertyDefinitions) {
            super(conjunction);
            this.keys = propertyDefinitions.stream()
                    .filter(propertyDefinition -> isPropertyOfType(propertyDefinition, dataType))
                    .map(PropertyDefinition::getPropertyName)
                    .collect(Collectors.toSet());

            if (this.keys.isEmpty()) {
                throw new GeException("Invalid query parameters, no properties of type " + dataType.getName() + " found");
            }
        }

        @Override
        public boolean isMatch(GeObject geObject) {
            for (Property prop : geObject.getProperties()) {
                if (this.keys.contains(prop.getName())) {
                    return true;
                }
            }
            return false;
        }

        public Iterable<String> getKeys() {
            return ImmutableSet.copyOf(this.keys);
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    ", keys='" + Joiner.on(", ").join(keys) + '\'' +
                    '}';
        }
    }

    public static class HasNotPropertyContainer extends HasContainer {
        private Set<String> keys;

        public HasNotPropertyContainer(Conjunction conjunction, String key) {
            super(conjunction);
            this.keys = Collections.singleton(key);
        }

        public HasNotPropertyContainer(Conjunction conjunction, Iterable<String> keys) {
            super(conjunction);
            this.keys = IterableUtils.toSet(keys);
        }

        public HasNotPropertyContainer(Conjunction conjunction, Class<? extends Value> dataType, Collection<PropertyDefinition> propertyDefinitions) {
            super(conjunction);
            this.keys = propertyDefinitions.stream()
                    .filter(propertyDefinition -> isPropertyOfType(propertyDefinition, dataType))
                    .map(PropertyDefinition::getPropertyName)
                    .collect(Collectors.toSet());

            if (this.keys.isEmpty()) {
                throw new GeException("Invalid query parameters, no properties of type " + dataType.getName() + " found");
            }
        }

        @Override
        public boolean isMatch(GeObject geObject) {
            for (Property prop : geObject.getProperties()) {
                if (this.keys.contains(prop.getName())) {
                    return false;
                }
            }
            return true;
        }

        public Iterable<String> getKeys() {
            return ImmutableSet.copyOf(this.keys);
        }

        @Override
        public String toString() {
            return this.getClass().getName() + "{" +
                    ", keys='" + Joiner.on(", ").join(keys) + '\'' +
                    '}';
        }
    }

    @Override
    public SimilarToGraphQuery minTermFrequency(int minTermFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new GeException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMinTermFrequency(minTermFrequency);
        return this;
    }

    @Override
    public SimilarToGraphQuery maxQueryTerms(int maxQueryTerms) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new GeException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMaxQueryTerms(maxQueryTerms);
        return this;
    }

    @Override
    public SimilarToGraphQuery minDocFrequency(int minDocFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new GeException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMinDocFrequency(minDocFrequency);
        return this;
    }

    @Override
    public SimilarToGraphQuery maxDocFrequency(int maxDocFrequency) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new GeException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setMaxDocFrequency(maxDocFrequency);
        return this;
    }

    @Override
    public SimilarToGraphQuery boost(float boost) {
        if (!(parameters instanceof SimilarToQueryParameters)) {
            throw new GeException("Invalid query parameters, expected " + SimilarToQueryParameters.class.getName() + " found " + parameters.getClass().getName());
        }
        ((SimilarToQueryParameters) this.parameters).setBoost(boost);
        return this;
    }

    @Override
    public boolean isAggregationSupported(Aggregation aggregation) {
        return false;
    }

    @Override
    public Query addAggregation(Aggregation aggregation) {
        if (!isAggregationSupported(aggregation)) {
            throw new GeException("Aggregation " + aggregation.getClass().getName() + " is not supported");
        }
        this.aggregations.add(aggregation);
        return this;
    }

    public Collection<Aggregation> getAggregations() {
        return aggregations;
    }

    public Aggregation getAggregationByName(String aggregationName) {
        for (Aggregation agg : aggregations) {
            if (agg.getAggregationName().equals(aggregationName)) {
                return agg;
            }
        }
        return null;
    }

    protected FetchHints idFetchHintsToElementFetchHints(EnumSet<IdFetchHint> idFetchHints) {
        return idFetchHints.contains(IdFetchHint.INCLUDE_HIDDEN) ? FetchHints.ALL_INCLUDING_HIDDEN : FetchHints.ALL;
    }

    @Override
    public Query setShard(String shardId) {
        return this;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{" +
                "parameters=" + getParameters() +
                '}';
    }
}
