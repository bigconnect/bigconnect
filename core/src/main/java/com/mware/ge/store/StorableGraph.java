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

import com.mware.ge.*;
import com.mware.ge.mutation.*;
import com.mware.ge.search.IndexHint;
import com.mware.ge.serializer.GeSerializer;

import java.util.List;

public interface StorableGraph<V extends StorableVertex, E extends StorableEdge> extends GraphWithSearchIndex {
    void invalidateElementFromCache(ElementType elementType, String id);
    void deleteProperty(StorableElement element, Property property, Authorizations authorizations);
    void softDeleteProperty(StorableElement element, Property property, Authorizations authorizations);
    void softDeleteProperties(Iterable<Property> properties, StorableElement element, Authorizations authorizations);
    void markPropertyHidden(
            StorableElement element,
            Property property,
            Long timestamp,
            Visibility visibility,
            @SuppressWarnings("UnusedParameters") Authorizations authorizations
    );
    void markPropertyVisible(
            StorableElement element,
            Property property,
            Long timestamp,
            Visibility visibility,
            Authorizations authorizations
    );
    void alterPropertyMetadatas(StorableElement element, List<SetPropertyMetadata> setPropertyMetadatas);
    void alterElementPropertyVisibilities(StorableElement element, List<AlterPropertyVisibility> alterPropertyVisibilities);
    void saveProperties(
            StorableElement element,
            Iterable<Property> properties,
            Iterable<PropertyDeleteMutation> propertyDeletes,
            Iterable<PropertySoftDeleteMutation> propertySoftDeletes
    );
    void alterElementVisibility(StorableElement element, Visibility newVisibility, Authorizations authorizations);
    void alterEdgeLabel(E edge, String newEdgeLabel, Authorizations authorizations);
    void alterConceptType(V vertex, String newConceptType);
    void saveExtendedDataMutations(
            Element element,
            ElementType elementType,
            IndexHint indexHint,
            Iterable<ExtendedDataMutation> extendedData,
            Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
            Authorizations authorizations
    );
    Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(
            Element element,
            String key,
            String name,
            Visibility visibility,
            Long startTime,
            Long endTime,
            Authorizations authorizations
    );
    GeSerializer getGeSerializer();
}
