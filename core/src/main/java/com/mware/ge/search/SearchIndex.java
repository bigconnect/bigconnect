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
import com.mware.ge.query.*;
import com.mware.ge.property.PropertyDescriptor;

import java.util.Collection;
import java.util.Set;

public interface SearchIndex {
    String EDGE_LABEL_FIELD_NAME = "__edgeLabel";
    String CONCEPT_TYPE_FIELD_NAME = "__conceptType";

    void addElement(
            Graph graph,
            Element element,
            Authorizations authorizations
    );

    <TElement extends Element> void updateElement(Graph graph, ExistingElementMutation<TElement> mutation, Authorizations authorizations);

    void deleteElement(Graph graph, ElementId elementId, Authorizations authorizations);

    default void deleteElements(Graph graph, Iterable<? extends ElementId> elementIds, Authorizations authorizations) {
        for (ElementId elementId : elementIds) {
            deleteElement(graph, elementId, authorizations);
        }
    }

    void markElementHidden(Graph graph, Element element, Visibility visibility, Authorizations authorizations);

    void markElementVisible(
            Graph graph,
            ElementLocation elementLocation,
            Visibility visibility,
            Authorizations authorizations
    );

    void markPropertyHidden(
            Graph graph,
            ElementLocation elementLocation,
            Property property,
            Visibility visibility,
            Authorizations authorizations
    );

    void markPropertyVisible(
            Graph graph,
            ElementLocation elementLocation,
            Property property,
            Visibility visibility,
            Authorizations authorizations
    );

    /**
     * Default delete property simply calls deleteProperty in a loop. It is up to the SearchIndex implementation to decide
     * if a collective method can be made more efficient
     */
    default void deleteProperties(
            Graph graph,
            Element element,
            Collection<PropertyDescriptor> propertyList,
            Authorizations authorizations)
    {
        propertyList.forEach(p -> deleteProperty(graph, element, p, authorizations));
    }

    void deleteProperty(
            Graph graph,
            Element element,
            PropertyDescriptor property,
            Authorizations authorizations
    );

    void addElements(Graph graph, Iterable<? extends Element> elements, Authorizations authorizations);

    GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations);

    MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, Authorizations authorizations);

    VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations);

    Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations);

    SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, Authorizations authorizations);

    void flush(Graph graph);

    void shutdown();

    void clearCache();

    boolean isFieldBoostSupported();

    void truncate(Graph graph);

    void drop(Graph graph);

    SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    boolean isQuerySimilarToTextSupported();

    boolean isFieldLevelSecuritySupported();

    <T extends Element> void alterElementVisibility(
            Graph graph,
            ExistingElementMutation<T> elementMutation,
            Visibility oldVisibility,
            Visibility newVisibility,
            Authorizations authorizations
    );

    void addElementExtendedData(
            Graph graph,
            ElementLocation elementLocation,
            Iterable<ExtendedDataMutation> extendedDatas,
            Authorizations authorizations
    );

    void addExtendedData(
            Graph graph,
            ElementLocation elementLocation,
            Iterable<ExtendedDataRow> extendedDatas,
            Authorizations authorizations
    );

    void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, Authorizations authorizations);

    void deleteExtendedData(
            Graph graph,
            ElementLocation elementLocation,
            String tableName,
            String row,
            String columnName,
            String key,
            Visibility visibility,
            Authorizations authorizations
    );

    void enableBulkIngest(boolean enable);

    int getNumShards();
}
