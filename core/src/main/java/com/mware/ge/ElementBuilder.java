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
package com.mware.ge;

import com.google.common.collect.ImmutableSet;
import com.mware.ge.mutation.*;
import com.mware.ge.property.MutablePropertyImpl;
import com.mware.ge.search.IndexHint;
import com.mware.ge.time.Clocks;
import com.mware.ge.util.IncreasingTime;
import com.mware.ge.util.Preconditions;
import com.mware.ge.util.StreamUtils;
import com.mware.ge.values.storable.Value;

import java.util.ArrayList;
import java.util.List;

public abstract class ElementBuilder<T extends Element> implements ElementMutation<T> {
    protected final List<Property> properties = new ArrayList<>();
    protected final List<PropertyDeleteMutation> propertyDeletes = new ArrayList<>();
    protected final List<PropertySoftDeleteMutation> propertySoftDeletes = new ArrayList<>();
    protected final List<ExtendedDataMutation> extendedDatas = new ArrayList<>();
    protected final List<ExtendedDataDeleteMutation> extendedDataDeletes = new ArrayList<>();
    protected String elementId;
    protected final ElementType elementType;
    private final Visibility elementVisibility;
    protected IndexHint indexHint = IndexHint.INDEX;

    protected ElementBuilder(ElementType elementType, String elementId, Visibility elementVisibility) {
        this.elementType = elementType;
        this.elementId = elementId;
        this.elementVisibility = elementVisibility;
    }

    @Override
    public String getId() {
        return elementId;
    }

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     * <p>
     * The added property will also be indexed in the configured search provider. The type of the value
     * will determine how it gets indexed.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> setProperty(String name, Value value, Visibility visibility) {
        return setProperty(name, value, Metadata.create(FetchHints.ALL), visibility);
    }

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     * <p>
     * The added property will also be indexed in the configured search provider. The type of the value
     * will determine how it gets indexed.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> setProperty(String name, Value value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(ElementMutation.DEFAULT_KEY, name, value, metadata, visibility);
    }

    /**
     * Adds or updates a property.
     * <p>
     * The added property will also be indexed in the configured search provider. The type of the value
     * will determine how it gets indexed.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> addPropertyValue(String key, String name, Value value, Visibility visibility) {
        return addPropertyValue(key, name, value, Metadata.create(FetchHints.ALL), visibility);
    }

    /**
     * Adds or updates a property.
     * <p>
     * The added property will also be indexed in the configured search provider. The type of the value
     * will determine how it gets indexed.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    public ElementBuilder<T> addPropertyValue(String key, String name, Value value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(key, name, value, metadata, IncreasingTime.currentTimeMillis(), visibility);
    }

    @Override
    public ElementBuilder<T> addPropertyValue(String key, String name, Value value, Metadata metadata, Long timestamp, Visibility visibility) {
        if (name == null) {
            throw new NullPointerException("property name cannot be null for property: " + name + ":" + key);
        }
        if (value == null) {
            throw new NullPointerException("property value cannot be null for property: " + name + ":" + key);
        }
        this.properties.add(new MutablePropertyImpl(
                key,
                name,
                value,
                metadata,
                timestamp,
                null,
                visibility,
                FetchHints.ALL_INCLUDING_HIDDEN
        ));
        return this;
    }

    @Override
    public ElementBuilder<T> deleteProperty(Property property) {
        propertyDeletes.add(new PropertyPropertyDeleteMutation(property));
        return this;
    }

    @Override
    public ElementBuilder<T> deleteProperty(String name, Visibility visibility) {
        return deleteProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @Override
    public ElementBuilder<T> deleteProperty(String key, String name, Visibility visibility) {
        Preconditions.checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        propertyDeletes.add(new KeyNameVisibilityPropertyDeleteMutation(key, name, visibility));
        return this;
    }

    @Override
    public ElementBuilder<T> softDeleteProperty(Property property) {
        propertySoftDeletes.add(new PropertyPropertySoftDeleteMutation(property));
        return this;
    }

    @Override
    public ElementBuilder<T> softDeleteProperty(String name, Visibility visibility) {
        return softDeleteProperty(ElementMutation.DEFAULT_KEY, name, visibility);
    }

    @Override
    public ElementBuilder<T> softDeleteProperty(String key, String name, Visibility visibility) {
        Preconditions.checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        propertySoftDeletes.add(new KeyNameVisibilityPropertySoftDeleteMutation(key, name, visibility));
        return this;
    }

    @Override
    public ElementBuilder<T> addExtendedData(String tableName, String row, String column, Value value, Visibility visibility) {
        return addExtendedData(tableName, row, column, null, value, visibility);
    }

    @Override
    public ElementMutation<T> addExtendedData(String tableName, String row, String column, Value value, Long timestamp, Visibility visibility) {
        return addExtendedData(tableName, row, column, null, value, timestamp, visibility);
    }

    @Override
    public ElementBuilder<T> addExtendedData(String tableName, String row, String column, String key, Value value, Visibility visibility) {
        return addExtendedData(tableName, row, column, key, value, IncreasingTime.currentTimeMillis(), visibility);
    }

    @Override
    public ElementBuilder<T> addExtendedData(String tableName, String row, String column, String key, Value value, Long timestamp, Visibility visibility) {
        this.extendedDatas.add(new ExtendedDataMutation(tableName, row, column, key, value, timestamp, visibility));
        return this;
    }

    @Override
    public ElementBuilder<T> deleteExtendedData(String tableName, String row, String column, Visibility visibility) {
        return deleteExtendedData(tableName, row, column, null, visibility);
    }

    @Override
    public ElementMutation<T> deleteExtendedData(String tableName, String row) {
        return deleteExtendedData(tableName, row, null, null);
    }

    @Override
    public ElementMutation<T> deleteExtendedDataTable(String tableName) {
        return deleteExtendedData(tableName, null, null, null);
    }

    @Override
    public ElementBuilder<T> deleteExtendedData(String tableName, String row, String column, String key, Visibility visibility) {
        extendedDataDeletes.add(new ExtendedDataDeleteMutation(tableName, row, column, key, visibility));
        return this;
    }

    @Override
    public ElementType getElementType() {
        return elementType;
    }

    @Override
    public Visibility getVisibility() {
        return elementVisibility;
    }



    /**
     * saves the element to the graph.
     *
     * @return either the vertex or edge just saved.
     */
    public abstract T save(Authorizations authorizations);

    @Override
    public Iterable<Property> getProperties() {
        return properties;
    }

    @Override
    public Iterable<PropertyDeleteMutation> getPropertyDeletes() {
        return propertyDeletes;
    }

    @Override
    public Iterable<PropertySoftDeleteMutation> getPropertySoftDeletes() {
        return propertySoftDeletes;
    }

    @Override
    public Iterable<ExtendedDataMutation> getExtendedData() {
        return extendedDatas;
    }

    @Override
    public Iterable<ExtendedDataDeleteMutation> getExtendedDataDeletes() {
        return extendedDataDeletes;
    }

    @Override
    public IndexHint getIndexHint() {
        return indexHint;
    }

    public ImmutableSet<String> getExtendedDataTableNames() {
        return extendedDatas.stream()
                .map(ExtendedDataMutation::getTableName)
                .collect(StreamUtils.toImmutableSet());
    }


    public ElementBuilder<T> overrideProperties(List<Property> properties) {
        this.properties.clear();
        this.properties.addAll(properties);

        return this;
    }

    public ElementBuilder<T> overridePropertyDeletes(List<PropertyDeleteMutation> propertyDeletes) {
        this.propertyDeletes.clear();
        this.propertyDeletes.addAll(propertyDeletes);

        return this;
    }

    public ElementBuilder<T> overridePropertySoftDeletes(List<PropertySoftDeleteMutation> propertySoftDeletes) {
        this.propertySoftDeletes.clear();
        this.propertySoftDeletes.addAll(propertySoftDeletes);

        return this;
    }

    public ElementBuilder<T> overrideExtendedDatas(List<ExtendedDataMutation> extendedDatas) {
        this.extendedDatas.clear();
        this.extendedDatas.addAll(extendedDatas);

        return this;
    }

    public ElementBuilder<T> overrideExtendedDataDeletes(List<ExtendedDataDeleteMutation> extendedDataDeletes) {
        this.extendedDataDeletes.clear();
        this.extendedDataDeletes.addAll(extendedDataDeletes);

        return this;
    }

    public ElementBuilder<T> overrideIndexHint(IndexHint indexHint) {
        this.indexHint = indexHint;

        return this;
    }

    @Override
    public ElementMutation<T> setIndexHint(IndexHint indexHint) {
        this.indexHint = indexHint;
        return this;
    }

    @Override
    public boolean hasChanges() {
        if (properties.size() > 0) {
            return true;
        }

        if (propertyDeletes.size() > 0) {
            return true;
        }

        if (propertySoftDeletes.size() > 0) {
            return true;
        }

        if (extendedDatas.size() > 0) {
            return true;
        }

        if (extendedDataDeletes.size() > 0) {
            return true;
        }

        return false;
    }

    public void setId(String id) {
        this.elementId = id;
    }
}
