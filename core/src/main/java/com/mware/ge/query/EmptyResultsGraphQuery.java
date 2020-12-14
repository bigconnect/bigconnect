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
import com.mware.ge.scoring.ScoringStrategy;
import com.mware.ge.sorting.SortingStrategy;
import com.mware.ge.values.storable.Value;
import org.apache.poi.ss.formula.functions.T;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

public class EmptyResultsGraphQuery implements Query {
    private List<Aggregation> aggregations = new ArrayList<>();

    @Override
    public QueryResultsIterable<Vertex> vertices() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Vertex> vertices(final FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> vertexIds() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> vertexIds(EnumSet<IdFetchHint> idFetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Edge> edges() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Edge> edges(final FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> edgeIds() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> edgeIds(EnumSet<IdFetchHint> idFetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    @Deprecated
    public QueryResultsIterable<Edge> edges(final String label) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    @Deprecated
    public QueryResultsIterable<Edge> edges(final String label, final FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Element> elements() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<Element> elements(final FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> elementIds() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<String> elementIds(EnumSet<IdFetchHint> idFetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<ExtendedDataRow> extendedDataRows(FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds(EnumSet<IdFetchHint> idFetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<? extends GeObject> search(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints) {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public QueryResultsIterable<? extends GeObject> search() {
        return new EmptyResultsQueryResultsIterable<>();
    }

    @Override
    public <T extends Value> Query range(String propertyName, T startValue, T endValue) {
        return this;
    }

    @Override
    public <T extends Value> Query range(String propertyName, T startValue, boolean inclusiveStartValue, T endValue, boolean inclusiveEndValue) {
        return this;
    }

    @Override
    public Query hasId(String... ids) {
        return this;
    }

    @Override
    public Query hasId(Iterable<String> ids) {
        return this;
    }

    @Override
    public Query hasEdgeLabel(String... edgeLabels) {
        return this;
    }

    @Override
    public Query hasEdgeLabel(Collection<String> edgeLabels) {
        return this;
    }

    @Override
    public Query hasConceptType(String... conceptTypes) {
        return this;
    }

    @Override
    public Query hasConceptType(Collection<String> conceptTypes) {
        return this;
    }

    @Override
    public Query hasAuthorization(String... authorizations) {
        return this;
    }

    @Override
    public Query hasAuthorization(Iterable<String> authorizations) {
        return this;
    }

    @Override
    public Query hasExtendedData(ElementType elementType, String elementId) {
        return this;
    }

    @Override
    public Query hasExtendedData(String tableName) {
        return this;
    }

    @Override
    public Query hasExtendedData(ElementType elementType, String elementId, String tableName) {
        return this;
    }

    @Override
    public Query hasExtendedData(Iterable<HasExtendedDataFilter> filters) {
        return this;
    }

    @Override
    public <T extends Value> Query has(String propertyName, T value) {
        return this;
    }

    @Override
    public <T extends Value> Query has(String propertyName, Predicate predicate, Conjunction conjunction, T value) {
        return this;
    }

    @Override
    public <T extends Value> Query hasNot(String propertyName, T value) {
        return this;
    }

    @Override
    public <T extends Value> Query has(String propertyName, Predicate predicate, T value) {
        return this;
    }

    @Override
    public <T extends Value, K extends Value> Query has(Class<T> dataType, Predicate predicate, K value) {
        return this;
    }

    @Override
    public <T extends Value> Query has(Class<T> dataType) {
        return this;
    }

    @Override
    public Query hasNot(Class dataType) {
        return this;
    }

    @Override
    public <T> Query has(Iterable<String> propertyNames) {
        return this;
    }

    @Override
    public <T> Query hasNot(Iterable<String> propertyNames) {
        return this;
    }

    @Override
    public <T extends Value> Query has(Iterable<String> propertyNames, Predicate predicate, T value) {
        return this;
    }

    @Override
    public Query has(String propertyName) {
        return this;
    }

    @Override
    public Query hasNot(String propertyName) {
        return this;
    }

    @Override
    public Query skip(int count) {
        return this;
    }

    @Override
    public Query limit(Integer count) {
        return this;
    }

    @Override
    public Query limit(Long count) {
        return this;
    }

    @Override
    public Query minScore(double score) {
        return this;
    }

    @Override
    public Query scoringStrategy(ScoringStrategy scoringStrategy) {
        return this;
    }

    @Override
    public Query sort(String propertyName, SortDirection direction) {
        return this;
    }

    @Override
    public Query sort(SortingStrategy sortingStrategy, SortDirection direction) {
        return this;
    }

    @Override
    public boolean isAggregationSupported(Aggregation aggregation) {
        return false;
    }

    @Override
    public Query addAggregation(Aggregation aggregation) {
        aggregations.add(aggregation);
        return this;
    }

    @Override
    public Iterable<Aggregation> getAggregations() {
        return aggregations;
    }

    @Override
    public Query setShard(String shardId) {
        return this;
    }
}
