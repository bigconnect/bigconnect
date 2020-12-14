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

import com.google.common.collect.Lists;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.property.MutablePropertyImpl;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.FilterIterable;
import com.mware.ge.values.storable.Value;

import java.util.Iterator;

import static com.mware.ge.values.storable.Values.stringValue;

public abstract class ExtendedDataRowBase implements ExtendedDataRow {
    private final FetchHints fetchHints;
    private transient Property rowIdProperty;
    private transient Property tableNameProperty;
    private transient Property elementIdProperty;
    private transient Property elementTypeProperty;

    protected ExtendedDataRowBase(FetchHints fetchHints) {
        this.fetchHints = fetchHints;
    }

    @Override
    public abstract ExtendedDataRowId getId();

    @Override
    public Iterable<String> getPropertyNames() {
        return new ConvertingIterable<Property, String>(getProperties()) {
            @Override
            protected String convert(Property prop) {
                return prop.getName();
            }
        };
    }

    @Override
    public abstract Iterable<Property> getProperties();

    @Override
    public Property getProperty(String name) {
        return getProperty(null, name, null);
    }

    @Override
    public Value getPropertyValue(String name) {
        return getPropertyValue(null, name);
    }

    @Override
    public Property getProperty(String key, String name, Visibility visibility) {
        if (ExtendedDataRow.ROW_ID.equals(name)) {
            return getRowIdProperty();
        } else if (ExtendedDataRow.TABLE_NAME.equals(name)) {
            return getTableNameProperty();
        } else if (ExtendedDataRow.ELEMENT_ID.equals(name)) {
            return getElementIdProperty();
        } else if (ExtendedDataRow.ELEMENT_TYPE.equals(name)) {
            return getElementTypeProperty();
        }
        getFetchHints().assertPropertyIncluded(name);
        for (Property property : getProperties()) {
            if (isMatch(property, key, name, visibility)) {
                return property;
            }
        }
        return null;
    }

    private boolean isMatch(Property property, String key, String name, Visibility visibility) {
        if (name != null && !property.getName().equals(name)) {
            return false;
        }
        if (key != null && !property.getKey().equals(key)) {
            return false;
        }
        if (visibility != null && !property.getVisibility().equals(visibility)) {
            return false;
        }
        return true;
    }

    @Override
    public Property getProperty(String name, Visibility visibility) {
        return getProperty(null, name, visibility);
    }

    @Override
    public Iterable<Property> getProperties(String name) {
        return getProperties(null, name);
    }

    @Override
    public Iterable<Property> getProperties(String key, String name) {
        if (ExtendedDataRow.ROW_ID.equals(name)) {
            return Lists.newArrayList(getRowIdProperty());
        } else if (ExtendedDataRow.TABLE_NAME.equals(name)) {
            return Lists.newArrayList(getTableNameProperty());
        } else if (ExtendedDataRow.ELEMENT_ID.equals(name)) {
            return Lists.newArrayList(getElementIdProperty());
        } else if (ExtendedDataRow.ELEMENT_TYPE.equals(name)) {
            return Lists.newArrayList(getElementTypeProperty());
        }

        getFetchHints().assertPropertyIncluded(name);
        return new FilterIterable<Property>(getProperties()) {
            @Override
            protected boolean isIncluded(Property prop) {
                return isMatch(prop, key, name, null);
            }
        };
    }

    @Override
    public Value getPropertyValue(String key, String name) {
        Property prop = getProperty(key, name);
        if (prop == null) {
            return null;
        }
        return prop.getValue();
    }

    @Override
    public Value getPropertyValue(String name, int index) {
        return getPropertyValue(null, name, index);
    }

    @Override
    public Value getPropertyValue(String key, String name, int index) {
        if (ExtendedDataRow.ROW_ID.equals(name)) {
            return getRowIdProperty().getValue();
        } else if (ExtendedDataRow.TABLE_NAME.equals(name)) {
            return getTableNameProperty().getValue();
        } else if (ExtendedDataRow.ELEMENT_ID.equals(name)) {
            return getElementIdProperty().getValue();
        } else if (ExtendedDataRow.ELEMENT_TYPE.equals(name)) {
            return getElementTypeProperty().getValue();
        }

        Iterator<Value> values = getPropertyValues(key, name).iterator();
        while (values.hasNext() && index > 0) {
            values.next();
            index--;
        }
        if (!values.hasNext()) {
            return null;
        }
        return values.next();
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof ExtendedDataRow) {
            return getId().compareTo(((ExtendedDataRow) o).getId());
        }
        throw new ClassCastException("o must be an " + ExtendedDataRow.class.getName());
    }

    protected Property getRowIdProperty() {
        if (rowIdProperty == null) {
            rowIdProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.ROW_ID,
                    stringValue(getId().getRowId()),
                    null,
                    null,
                    null,
                    null,
                    FetchHints.ALL
            );
        }
        return rowIdProperty;
    }

    protected Property getTableNameProperty() {
        if (tableNameProperty == null) {
            tableNameProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.TABLE_NAME,
                    stringValue(getId().getTableName()),
                    null,
                    null,
                    null,
                    null,
                    FetchHints.ALL
            );
        }
        return tableNameProperty;
    }

    protected Property getElementIdProperty() {
        if (elementIdProperty == null) {
            elementIdProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.ELEMENT_ID,
                    stringValue(getId().getElementId()),
                    null,
                    null,
                    null,
                    null,
                    FetchHints.ALL
            );
        }
        return elementIdProperty;
    }

    protected Property getElementTypeProperty() {
        if (elementTypeProperty == null) {
            elementTypeProperty = new MutablePropertyImpl(
                    ElementMutation.DEFAULT_KEY,
                    ExtendedDataRow.ELEMENT_TYPE,
                    stringValue(getId().getElementType().name()),
                    null,
                    null,
                    null,
                    null,
                    FetchHints.ALL
            );
        }
        return elementTypeProperty;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }
}
