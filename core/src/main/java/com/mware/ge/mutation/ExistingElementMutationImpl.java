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
package com.mware.ge.mutation;

import com.mware.ge.*;
import com.mware.ge.property.MutablePropertyImpl;
import com.mware.ge.search.IndexHint;
import com.mware.ge.util.IncreasingTime;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.Value;

import java.util.ArrayList;
import java.util.List;

public abstract class ExistingElementMutationImpl<T extends Element> implements ElementMutation<T>, ExistingElementMutation<T> {
    private final List<Property> properties = new ArrayList<>();
    private final List<PropertyDeleteMutation> propertyDeletes = new ArrayList<>();
    private final List<PropertySoftDeleteMutation> propertySoftDeletes = new ArrayList<>();
    private Visibility newElementVisibility;
    private Visibility oldElementVisibility;
    private final List<AlterPropertyVisibility> alterPropertyVisibilities = new ArrayList<>();
    private final List<SetPropertyMetadata> setPropertyMetadatas = new ArrayList<>();
    private final List<ExtendedDataMutation> extendedDatas = new ArrayList<>();
    private final List<ExtendedDataDeleteMutation> extendedDataDeletes = new ArrayList<>();
    private final T element;
    private final ElementType elementType;
    private IndexHint indexHint = IndexHint.INDEX;

    public ExistingElementMutationImpl(T element) {
        this.element = element;
        this.elementType = ElementType.getTypeFromElement(element);
        if (element != null) {
            this.oldElementVisibility = element.getVisibility();
        }
    }



    public ExistingElementMutationImpl overrideProperties(List<Property> properties) {
        this.properties.clear();
        this.properties.addAll(properties);
        return this;
    }

    public ExistingElementMutationImpl overridePropertyDeletes(List<PropertyDeleteMutation> propertyDeletes) {
        this.propertyDeletes.clear();
        this.propertyDeletes.addAll(propertyDeletes);
        return this;
    }

    public ExistingElementMutationImpl overridePropertySoftDeletes(List<PropertySoftDeleteMutation> propertySoftDeletes) {
        this.propertySoftDeletes.clear();
        this.propertySoftDeletes.addAll(propertySoftDeletes);
        return this;
    }

    public ExistingElementMutationImpl overrideAlterPropertyVisibilities(List<AlterPropertyVisibility> alterPropertyVisibilities) {
        this.alterPropertyVisibilities.clear();
        this.alterPropertyVisibilities.addAll(alterPropertyVisibilities);
        return this;
    }

    public ExistingElementMutationImpl overrideSetPropertyMetadatas(List<SetPropertyMetadata> setPropertyMetadatas) {
        this.setPropertyMetadatas.clear();
        this.setPropertyMetadatas.addAll(setPropertyMetadatas);
        return this;
    }

    public ExistingElementMutationImpl overrideExtendedDatas(List<ExtendedDataMutation> extendedDatas) {
        this.extendedDatas.clear();
        this.extendedDatas.addAll(extendedDatas);
        return this;
    }

    public ExistingElementMutationImpl overrideExtendedDataDeletes(List<ExtendedDataDeleteMutation> extendedDataDeletes) {
        this.extendedDataDeletes.clear();
        this.extendedDataDeletes.addAll(extendedDataDeletes);
        return this;
    }

    public ExistingElementMutationImpl overrideOldElementVisibility(Visibility oldElementVisibility) {
        this.oldElementVisibility = oldElementVisibility;
        return this;
    }

    public ExistingElementMutationImpl overrideNewElementVisibility(Visibility newElementVisibility) {
        this.newElementVisibility = newElementVisibility;
        return this;
    }

    public abstract T save(Authorizations authorizations);

    public ElementMutation<T> setProperty(String name, Value value, Visibility visibility) {
        return setProperty(name, value, Metadata.create(FetchHints.ALL), visibility);
    }

    public ElementMutation<T> setProperty(String name, Value value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(DEFAULT_KEY, name, value, metadata, visibility);
    }

    public ElementMutation<T> addPropertyValue(String key, String name, Value value, Visibility visibility) {
        return addPropertyValue(key, name, value, Metadata.create(FetchHints.ALL), visibility);
    }

    public ElementMutation<T> addPropertyValue(String key, String name, Value value, Metadata metadata, Visibility visibility) {
        return addPropertyValue(key, name, value, metadata, null, visibility);
    }

    @Override
    public ElementMutation<T> addPropertyValue(String key, String name, Value value, Metadata metadata, Long timestamp, Visibility visibility) {
        Preconditions.checkNotNull(name, "property name cannot be null for property: " + name + ":" + key);
        Preconditions.checkNotNull(value, "property value cannot be null for property: " + name + ":" + key);
        properties.add(new MutablePropertyImpl(key, name, value, metadata, timestamp, null, visibility, FetchHints.ALL_INCLUDING_HIDDEN));
        return this;
    }

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
    public ElementMutation<T> deleteProperty(Property property) {
        if (!element.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
            throw new GeMissingFetchHintException(element.getFetchHints(), "Property " + property.getName() + " needs to be included with metadata");
        }
        Preconditions.checkNotNull(property, "property cannot be null");
        propertyDeletes.add(new PropertyPropertyDeleteMutation(property));
        return this;
    }

    @Override
    public Iterable<ExtendedDataDeleteMutation> getExtendedDataDeletes() {
        return extendedDataDeletes;
    }

    @Override
    public ExistingElementMutation<T> deleteProperties(String name) {
        for (Property prop : this.element.getProperties(name)) {
            deleteProperty(prop);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> deleteProperties(String key, String name) {
        for (Property prop : this.element.getProperties(key, name)) {
            deleteProperty(prop);
        }
        return this;
    }

    @Override
    public ElementMutation<T> deleteProperty(String name, Visibility visibility) {
        Property property = this.element.getProperty(name, visibility);
        if (property != null) {
            deleteProperty(property);
        }
        return this;
    }

    @Override
    public ElementMutation<T> deleteProperty(String key, String name, Visibility visibility) {
        Property property = this.element.getProperty(key, name, visibility);
        if (property != null) {
            deleteProperty(property);
        }
        return this;
    }

    @Override
    public ElementMutation<T> softDeleteProperty(Property property) {
        Preconditions.checkNotNull(property, "property cannot be null");
        propertySoftDeletes.add(new PropertyPropertySoftDeleteMutation(property));
        return this;
    }

    @Override
    public ExistingElementMutation<T> softDeleteProperties(String name) {
        for (Property prop : this.element.getProperties(name)) {
            softDeleteProperty(prop);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> softDeleteProperties(String key, String name) {
        for (Property prop : this.element.getProperties(key, name)) {
            softDeleteProperty(prop);
        }
        return this;
    }

    @Override
    public ElementMutation<T> softDeleteProperty(String name, Visibility visibility) {
        Property property = this.element.getProperty(name, visibility);
        if (property != null) {
            softDeleteProperty(property);
        }
        return this;
    }

    @Override
    public ElementMutation<T> softDeleteProperty(String key, String name, Visibility visibility) {
        Property property = this.element.getProperty(key, name, visibility);
        if (property != null) {
            softDeleteProperty(property);
        }
        return this;
    }

    @Override
    public ExistingElementMutation<T> alterPropertyVisibility(Property property, Visibility visibility) {
        if (!element.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
            throw new GeMissingFetchHintException(element.getFetchHints(), "Property " + property.getName() + " needs to be included with metadata");
        }
        this.alterPropertyVisibilities.add(new AlterPropertyVisibility(property.getKey(), property.getName(), property.getVisibility(), visibility));
        return this;
    }

    @Override
    public ExistingElementMutation<T> alterPropertyVisibility(String name, Visibility visibility) {
        return alterPropertyVisibility(DEFAULT_KEY, name, visibility);
    }

    @Override
    public ExistingElementMutation<T> alterPropertyVisibility(String key, String name, Visibility visibility) {
        if (!element.getFetchHints().isIncludePropertyAndMetadata(name)) {
            throw new GeMissingFetchHintException(element.getFetchHints(), "Property " + name + " needs to be included with metadata");
        }
        this.alterPropertyVisibilities.add(new AlterPropertyVisibility(key, name, null, visibility));
        return this;
    }

    @Override
    public ExistingElementMutation<T> alterElementVisibility(Visibility visibility) {
        this.newElementVisibility = visibility;
        return this;
    }

    @Override
    public ExistingElementMutation<T> setPropertyMetadata(Property property, String metadataName, Value newValue, Visibility visibility) {
        this.setPropertyMetadatas.add(new SetPropertyMetadata(property.getKey(), property.getName(), property.getVisibility(), metadataName, newValue, visibility));
        return this;
    }

    @Override
    public ExistingElementMutation<T> setPropertyMetadata(String propertyName, String metadataName, Value newValue, Visibility visibility) {
        return setPropertyMetadata(DEFAULT_KEY, propertyName, metadataName, newValue, visibility);
    }

    @Override
    public ExistingElementMutation<T> setPropertyMetadata(String propertyKey, String propertyName, String metadataName, Value newValue, Visibility visibility) {
        this.setPropertyMetadatas.add(new SetPropertyMetadata(propertyKey, propertyName, null, metadataName, newValue, visibility));
        return this;
    }

    @Override
    public ExistingElementMutation<T> addExtendedData(String tableName, String row, String column, Value value, Visibility visibility) {
        return addExtendedData(tableName, row, column, null, value, null, visibility);
    }

    @Override
    public ExistingElementMutation<T> addExtendedData(String tableName, String row, String column, Value value, Long timestamp, Visibility visibility) {
        return addExtendedData(tableName, row, column, null, value, timestamp, visibility);
    }

    @Override
    public ExistingElementMutation<T> addExtendedData(String tableName, String row, String column, String key, Value value, Visibility visibility) {
        return addExtendedData(tableName, row, column, key, value, null, visibility);
    }

    @Override
    public ExistingElementMutation<T> addExtendedData(String tableName, String row, String column, String key, Value value, Long timestamp, Visibility visibility) {
        this.extendedDatas.add(new ExtendedDataMutation(tableName, row, column, key, value, timestamp, visibility));
        return this;
    }

    @Override
    public ExistingElementMutation<T> deleteExtendedData(String tableName, String row, String column, Visibility visibility) {
        return deleteExtendedData(tableName, row, column, null, visibility);
    }

    @Override
    public ElementMutation<T> deleteExtendedData(String tableName, String row) {
        return deleteExtendedData(tableName, row, null, null, null);
    }

    @Override
    public ElementMutation<T> deleteExtendedDataTable(String tableName) {
        return deleteExtendedData(tableName, null, null, null, null);
    }

    @Override
    public ExistingElementMutation<T> deleteExtendedData(String tableName, String row, String column, String key, Visibility visibility) {
        extendedDataDeletes.add(new ExtendedDataDeleteMutation(tableName, row, column, key, visibility));
        return this;
    }

    @Override
    public T getElement() {
        return element;
    }

    @Override
    public ElementType getElementType() {
        return elementType;
    }

    @Override
    public String getId() {
        return getElement().getId();
    }

    @Override
    public Visibility getVisibility() {
        return getElement().getVisibility();
    }

    @Override
    public Visibility getNewElementVisibility() {
        return newElementVisibility;
    }

    @Override
    public Visibility getOldElementVisibility() {
        return oldElementVisibility;
    }

    @Override
    public List<AlterPropertyVisibility> getAlterPropertyVisibilities() {
        return alterPropertyVisibilities;
    }

    @Override
    public List<SetPropertyMetadata> getSetPropertyMetadatas() {
        return setPropertyMetadatas;
    }

    @Override
    public IndexHint getIndexHint() {
        return indexHint;
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

        if (newElementVisibility != null) {
            return true;
        }

        if (alterPropertyVisibilities.size() > 0) {
            return true;
        }

        if (setPropertyMetadatas.size() > 0) {
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
}
