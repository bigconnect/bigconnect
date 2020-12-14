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

import com.google.common.collect.ImmutableSet;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.QueryableIterable;
import com.mware.ge.values.storable.Value;

public abstract class ElasticsearchElement extends ElementBase {
    private final Graph graph;
    private FetchHints fetchHints;
    private String id;
    private Authorizations authorizations;

    public ElasticsearchElement(
            Graph graph,
            String id,
            FetchHints fetchHints,
            Authorizations authorizations
    ) {
        this.id = id;
        this.graph = graph;
        this.fetchHints = fetchHints;
        this.authorizations = authorizations;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Iterable<Property> getProperties() {
        throw new GeNotSupportedException("getProperties is not supported");
    }

    @Override
    public Property getProperty(String name) {
        throw new GeNotSupportedException("getProperty is not supported");
    }

    @Override
    public Value getPropertyValue(String name) {
        throw new GeNotSupportedException("getPropertyValue is not supported");
    }

    @Override
    public Property getProperty(String key, String name) {
        throw new GeNotSupportedException("getProperty is not supported");
    }

    @Override
    public Iterable<Value> getPropertyValues(String name) {
        throw new GeNotSupportedException("getPropertyValues is not supported");
    }

    @Override
    public Iterable<Value> getPropertyValues(String key, String name) {
        throw new GeNotSupportedException("getPropertyValues is not supported");
    }

    @Override
    public Value getPropertyValue(String key, String name) {
        throw new GeNotSupportedException("getPropertyValue is not supported");
    }

    @Override
    public Value getPropertyValue(String name, int index) {
        throw new GeNotSupportedException("getPropertyValue is not supported");
    }

    @Override
    public Value getPropertyValue(String key, String name, int index) {
        throw new GeNotSupportedException("getPropertyValue is not supported");
    }

    @Override
    public Visibility getVisibility() {
        throw new GeNotSupportedException("getVisibility is not supported");
    }

    @Override
    public long getTimestamp() {
        throw new GeNotSupportedException("getTimestamp is not supported");
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Authorizations authorizations) {
        throw new GeNotSupportedException("getHistoricalPropertyValues is not supported");
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Long startTime, Long endTime, Authorizations authorizations) {
        throw new GeNotSupportedException("getHistoricalPropertyValues is not supported");
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("getHistoricalPropertyValues is not supported");
    }

    @Override
    public Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Long startTime, Long endTime, Authorizations authorizations) {
        throw new GeNotSupportedException("getHistoricalPropertyValues is not supported");
    }

    @Override
    public <T extends Element> ExistingElementMutation<T> prepareMutation() {
        throw new GeNotSupportedException("prepareMutation is not supported");
    }

    @Override
    public void deleteProperty(String key, String name, Authorizations authorizations) {
        throw new GeNotSupportedException("deleteProperty is not supported");
    }

    @Override
    public void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("deleteProperty is not supported");
    }

    @Override
    public void deleteProperties(String name, Authorizations authorizations) {
        throw new GeNotSupportedException("deleteProperties is not supported");
    }

    @Override
    public void softDeleteProperty(String key, String name, Authorizations authorizations) {
        throw new GeNotSupportedException("softDeleteProperty is not supported");
    }

    @Override
    public void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("softDeleteProperty is not supported");
    }

    @Override
    public void softDeleteProperties(String name, Authorizations authorizations) {
        throw new GeNotSupportedException("softDeleteProperties is not supported");
    }

    @Override
    public GraphWithSearchIndex getGraph() {
        return (GraphWithSearchIndex) graph;
    }

    @Override
    public void addPropertyValue(String key, String name, Value value, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("addPropertyValue is not supported");
    }

    @Override
    public void addPropertyValue(String key, String name, Value value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("addPropertyValue is not supported");
    }

    @Override
    public void setProperty(String name, Value value, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("setProperty is not supported");
    }

    @Override
    public void setProperty(String name, Value value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("setProperty is not supported");
    }

    @Override
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    @Override
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("markPropertyHidden is not supported");
    }

    @Override
    public void markPropertyHidden(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("markPropertyHidden is not supported");
    }

    @Override
    public void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("markPropertyHidden is not supported");
    }

    @Override
    public void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("markPropertyHidden is not supported");
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("markPropertyVisible is not supported");
    }

    @Override
    public void markPropertyVisible(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("markPropertyVisible is not supported");
    }

    @Override
    public void markPropertyVisible(Property property, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("markPropertyVisible is not supported");
    }

    @Override
    public void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations) {
        throw new GeNotSupportedException("markPropertyVisible is not supported");
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        throw new GeNotSupportedException("isHidden is not supported");
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        throw new GeNotSupportedException("getHiddenVisibilities is not supported");
    }

    @Override
    public ImmutableSet<String> getExtendedDataTableNames() {
        throw new GeNotSupportedException("getExtendedDataTableNames is not supported");
    }

    @Override
    public QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints) {
        throw new GeNotSupportedException("getExtendedData is not supported");
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }
}
