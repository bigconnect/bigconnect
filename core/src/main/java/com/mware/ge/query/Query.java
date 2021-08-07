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

import java.util.Collection;
import java.util.EnumSet;

public interface Query {
    QueryResultsIterable<Vertex> vertices();

    QueryResultsIterable<Vertex> vertices(FetchHints fetchHints);

    /**
     * Execute the query and return the ids of all matching vertices.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching vertices.
     * Hidden vertices are not included in the results.
     *
     * @return The ids of vertices that match this query.
     */
    QueryResultsIterable<String> vertexIds();

    /**
     * Execute the query and return the ids of all matching vertices.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching vertices.
     *
     * @param fetchHints Details about which data to fetch.
     * @return The ids of vertices that match this query.
     */
    QueryResultsIterable<String> vertexIds(EnumSet<IdFetchHint> fetchHints);

    QueryResultsIterable<Edge> edges();

    QueryResultsIterable<Edge> edges(FetchHints fetchHints);

    /**
     * Execute the query and return the ids of all matching edges.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching edges.
     * Hidden edges are not included in the results.
     *
     * @return The ids of edges that match this query.
     */
    QueryResultsIterable<String> edgeIds();

    /**
     * Execute the query and return the ids of all matching edges.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching edges.
     * Hidden edges are not included in the results.
     *
     * @param fetchHints Details about which data to fetch.
     * @return The ids of edges that match this query.
     */
    QueryResultsIterable<String> edgeIds(EnumSet<IdFetchHint> fetchHints);

    QueryResultsIterable<ExtendedDataRow> extendedDataRows();

    QueryResultsIterable<ExtendedDataRow> extendedDataRows(FetchHints fetchHints);

    /**
     * Execute the query and return the ids of all matching extended data rows.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching extended data rows.
     * Hidden extended data rows are not included in the results.
     *
     * @return The ids of extended data rows that match this query.
     */
    QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds();

    /**
     * Execute the query and return the ids of all matching extended data rows.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching extended data rows.
     * Hidden extended data rows are not included in the results.
     *
     * @param fetchHints Details about which data to fetch.
     * @return The ids of extended data rows that match this query.
     */
    QueryResultsIterable<ExtendedDataRowId> extendedDataRowIds(EnumSet<IdFetchHint> fetchHints);

    QueryResultsIterable<Element> elements();

    QueryResultsIterable<Element> elements(FetchHints fetchHints);

    /**
     * Execute the query and return the ids of all matching elements.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching elements.
     * Hidden elements are not included in the results.
     *
     * @return The ids of elements that match this query.
     */
    QueryResultsIterable<String> elementIds();

    /**
     * Execute the query and return the ids of all matching elements.
     * This method will not load the data from storage, which is more
     * efficient in cases where you only need to know the ids of matching elements.
     * Hidden elements are not included in the results.
     *
     * @return The ids of elements that match this query.
     */
    QueryResultsIterable<String> elementIds(EnumSet<IdFetchHint> fetchHints);

    QueryResultsIterable<? extends GeObject> search(EnumSet<GeObjectType> objectTypes, FetchHints fetchHints);

    QueryResultsIterable<? extends GeObject> search();

    /**
     * Test to see if aggregation is supported.
     *
     * @param aggregation the aggregation to test.
     * @return true, if the aggregation is supported
     */
    boolean isAggregationSupported(Aggregation aggregation);

    /**
     * Add an aggregation to the query
     *
     * @param aggregation the aggregation to add.
     * @return The query object, allowing you to chain methods.
     */
    Query addAggregation(Aggregation aggregation);

    /**
     * Gets the added aggregations
     */
    Iterable<Aggregation> getAggregations();

    Query setShard(String shardId);

    Authorizations getAuthorizations();

    interface SortContainer {
    }
}
