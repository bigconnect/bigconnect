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
package com.mware.ge.search;

import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.mutation.ExtendedDataMutation;
import com.mware.ge.property.PropertyDescriptor;
import com.mware.ge.query.*;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.util.Preconditions;

import static com.mware.ge.util.Preconditions.checkNotNull;

public class DefaultSearchIndex implements SearchIndex {
    @SuppressWarnings("unused")
    public DefaultSearchIndex(GraphConfiguration configuration) {

    }

    @Override
    public void addElement(Graph graph, Element element, Authorizations authorizations) {
        Preconditions.checkNotNull(element, "element cannot be null");
    }

    @Override
    public <TElement extends Element> void updateElement(Graph graph, ExistingElementMutation<TElement> mutation, Authorizations authorizations) {
        checkNotNull(mutation, "mutation cannot be null");
    }

    @Override
    public void markElementHidden(Graph graph, Element element, Visibility visibility, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void markElementVisible(
            Graph graph,
            ElementLocation elementLocation,
            Visibility visibility,
            Authorizations authorizations
    ) {
        checkNotNull(elementLocation, "elementLocation cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void markPropertyHidden(
            Graph graph,
            ElementLocation elementLocation,
            Property property,
            Visibility visibility,
            Authorizations authorizations
    ) {
        checkNotNull(elementLocation, "elementLocation cannot be null");
        checkNotNull(property, "property cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void markPropertyVisible(
            Graph graph,
            ElementLocation elementLocation,
            Property property,
            Visibility visibility,
            Authorizations authorizations
    ) {
        checkNotNull(elementLocation, "elementLocation cannot be null");
        checkNotNull(property, "property cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public <T extends Element> void alterElementVisibility(
            Graph graph,
            ExistingElementMutation<T> elementMutation,
            Visibility oldVisibility,
            Visibility newVisibility,
            Authorizations authorizations
    ) {
        checkNotNull(elementMutation, "elementMutation cannot be null");
        checkNotNull(newVisibility, "newVisibility cannot be null");
    }

    @Override
    public void deleteElement(Graph graph, ElementId element, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
    }

    @Override
    public void deleteElements(Graph graph, Iterable<? extends ElementId> elementIds, Authorizations authorizations) {
        checkNotNull(elementIds, "element cannot be null");
    }

    @Override
    public void deleteProperty(Graph graph, Element element, PropertyDescriptor property, Authorizations authorizations) {
        checkNotNull(element, "element cannot be null");
    }

    @Override
    public void addElements(Graph graph, Iterable<? extends Element> elements, Authorizations authorizations) {
        for (Element element : elements) {
            addElement(graph, element, authorizations);
        }
    }

    @Override
    public Query queryGraph(Graph graph, GeQueryBuilder queryBuilder, Authorizations authorizations) {
        return new DefaultGraphQuery(graph, queryBuilder, authorizations);
    }

    @Override
    public VertexQuery queryVertex(Graph graph, Vertex vertex, GeQueryBuilder queryBuilder, Authorizations authorizations) {
        return new DefaultVertexQuery(graph, vertex, queryBuilder, authorizations);
    }

    @Override
    public void flush(Graph graph) {

    }

    @Override
    public void clearCache() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean isFieldBoostSupported() {
        return false;
    }

    @Override
    public void truncate(Graph graph) {
    }

    @Override
    public void drop(Graph graph) {

    }

    public SearchIndexSecurityGranularity getSearchIndexSecurityGranularity() {
        return SearchIndexSecurityGranularity.PROPERTY;
    }

    @Override
    public boolean isFieldLevelSecuritySupported() {
        return true;
    }

    @Override
    public void addElementExtendedData(
            Graph graph,
            ElementLocation elementLocation,
            Iterable<ExtendedDataMutation> extendedDatas,
            Authorizations authorizations
    ) {
    }

    @Override
    public void addExtendedData(
            Graph graph,
            ElementLocation elementLocation,
            Iterable<ExtendedDataRow> extendedDatas,
            Authorizations authorizations
    ) {
    }

    @Override
    public void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, Authorizations authorizations) {
        checkNotNull(extendedDataRowId, "extendedDataRowId cannot be null");
    }

    @Override
    public Query queryExtendedData(Graph graph, Element element, String tableName, GeQueryBuilder queryBuilder, Authorizations authorizations) {
        return new DefaultExtendedDataQuery(graph, element, tableName, queryBuilder, authorizations);
    }

    @Override
    public void deleteExtendedData(
            Graph graph,
            ElementLocation elementLocation,
            String tableName,
            String row,
            String columnName,
            String key,
            Visibility visibility,
            Authorizations authorizations
    ) {
        checkNotNull(elementLocation, "elementLocation cannot be null");
        checkNotNull(tableName, "tableName cannot be null");
        checkNotNull(row, "row cannot be null");
        checkNotNull(columnName, "columnName cannot be null");
        checkNotNull(visibility, "visibility cannot be null");
    }

    @Override
    public void enableBulkIngest(boolean enable) {

    }

    @Override
    public int getNumShards() {
        return 1;
    }
}
