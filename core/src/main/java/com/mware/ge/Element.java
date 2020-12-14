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
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.QueryableIterable;
import com.mware.ge.util.FilterIterable;
import com.mware.ge.values.storable.Value;

/**
 * An element on the graph. This can be either a vertex or edge.
 * <p/>
 * Elements also contain properties. These properties are unique given their key, name, and visibility.
 * For example a property with key "key1" and name "age" could have to values, one with visibility "a" and one
 * with visibility "b".
 */
public interface Element extends GeObject, ElementLocation {
    /**
     * Meta property name used for operations such as sorting
     */
    String ID_PROPERTY_NAME = "__ID__";

    /**
     * the visibility of the element.
     */
    Visibility getVisibility();

    /**
     * The timestamp of when this element was updated.
     */
    long getTimestamp();

    default Long getEndTime() {
        return null;
    }

    /**
     * Gets all property values from all timestamps in descending timestamp order.
     */
    default Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(Authorizations authorizations) {
        return getHistoricalPropertyValues(null, null, authorizations);
    }

    /**
     * Gets all property values from the given range of timestamps in descending timestamp order.
     */
    default Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(final Long startTime, final Long endTime, Authorizations authorizations) {
        return getHistoricalPropertyValues(null, null, null, startTime, endTime, authorizations);
    }

    /**
     * Gets property values from all timestamps in descending timestamp order.
     *
     * @param key        the key of the property.
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     */
    default Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(String key, String name, Visibility visibility, Authorizations authorizations) {
        return getHistoricalPropertyValues(key, name, visibility, null, null, authorizations);
    }

    /**
     * Gets property values from the given range of timestamps in descending timestamp order.
     *
     * @param key        the key of the property.
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     */
    default Iterable<HistoricalPropertyValue> getHistoricalPropertyValues(
            String key,
            String name,
            Visibility visibility,
            Long startTime,
            Long endTime,
            Authorizations authorizations
    ) {
        return new FilterIterable<HistoricalPropertyValue>(getHistoricalPropertyValues(key, name, visibility, authorizations)) {
            @Override
            protected boolean isIncluded(HistoricalPropertyValue pv) {
                if (startTime != null && pv.getTimestamp() < startTime) {
                    return false;
                }
                if (endTime != null && pv.getTimestamp() > endTime) {
                    return false;
                }
                return true;
            }
        };
    }

    /**
     * Prepares a mutation to allow changing multiple property values at the same time. This method is similar to
     * Graph#prepareVertex(Visibility, Authorizations)
     * in that it allows multiple properties to be changed and saved in a single mutation.
     *
     * @return The mutation builder.
     */
    <T extends Element> ExistingElementMutation<T> prepareMutation();

    /**
     * Permanently deletes a property from the element. Only properties which you have access to can be deleted using
     * this method.
     *
     * @param property The property to delete.
     */
    default void deleteProperty(Property property, Authorizations authorizations) {
        deleteProperty(property.getKey(), property.getName(), property.getVisibility(), authorizations);
    }

    /**
     * Permanently deletes a property given it's key and name from the element. Only properties which you have access
     * to can be deleted using this method.
     *
     * @param key  The property key.
     * @param name The property name.
     */
    default void deleteProperty(String key, String name, Authorizations authorizations) {
        deleteProperty(key, name, null, authorizations);
    }

    /**
     * Permanently deletes a property given it's key and name from the element. Only properties which you have access
     * to can be deleted using this method.
     *
     * @param key        The property key.
     * @param name       The property name.
     * @param visibility The property visibility.
     */
    void deleteProperty(String key, String name, Visibility visibility, Authorizations authorizations);

    /**
     * Permanently deletes all properties with the given name that you have access to. Only properties which you have
     * access to will be deleted.
     *
     * @param name The name of the property to delete.
     */
    default void deleteProperties(String name, Authorizations authorizations) {
        for (Property p : getProperties(name)) {
            deleteProperty(p.getKey(), p.getName(), p.getVisibility(), authorizations);
        }
    }

    /**
     * Soft deletes a property given it's key and name from the element. Only properties which you have access
     * to can be soft deleted using this method.
     *
     * @param key  The property key.
     * @param name The property name.
     */
    default void softDeleteProperty(String key, String name, Authorizations authorizations) {
        softDeleteProperty(key, name, null, authorizations);
    }

    /**
     * Soft deletes a property given it's key and name from the element for a given visibility. Only properties which you have access
     * to can be soft deleted using this method.
     *
     * @param key        The property key.
     * @param name       The property name.
     * @param visibility The visibility string of the property to soft delete.
     */
    void softDeleteProperty(String key, String name, Visibility visibility, Authorizations authorizations);

    /**
     * Soft deletes all properties with the given name that you have access to. Only properties which you have
     * access to will be soft deleted.
     *
     * @param name The name of the property to delete.
     */
    default void softDeleteProperties(String name, Authorizations authorizations) {
        for (Property property : getProperties(name)) {
            softDeleteProperty(property.getKey(), property.getName(), property.getVisibility(), authorizations);
        }
    }

    /**
     * Gets the graph that this element belongs to.
     */
    Graph getGraph();

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    default void addPropertyValue(String key, String name, Value value, Visibility visibility, Authorizations authorizations) {
        prepareMutation()
                .addPropertyValue(key, name, value, visibility)
                .save(authorizations);
    }

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    default void addPropertyValue(String key, String name, Value value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        prepareMutation()
                .addPropertyValue(key, name, value, metadata, visibility)
                .save(authorizations);
    }

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    default void setProperty(String name, Value value, Visibility visibility, Authorizations authorizations) {
        prepareMutation()
                .setProperty(name, value, visibility)
                .save(authorizations);
    }

    /**
     * Sets or updates a property value. The property key will be set to a constant. This is a convenience method
     * which allows treating the multi-valued nature of properties as only containing a single value. Care must be
     * taken when using this method because properties are not only uniquely identified by just key and name but also
     * visibility so adding properties with the same name and different visibility strings is still permitted.
     *
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    default void setProperty(String name, Value value, Metadata metadata, Visibility visibility, Authorizations authorizations) {
        prepareMutation()
                .setProperty(name, value, metadata, visibility)
                .save(authorizations);
    }

    /**
     * Gets the authorizations used to get this element.
     */
    Authorizations getAuthorizations();

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param visibility         The visibility string under which this property is hidden.
     *                           This visibility can be a superset of the property visibility to mark
     *                           it as hidden for only a subset of authorizations.
     * @param authorizations     The authorizations used.
     */
    default void markPropertyHidden(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(key, name, propertyVisibility, null, visibility, authorizations);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param timestamp          The timestamp.
     * @param visibility         The visibility string under which this property is hidden.
     *                           This visibility can be a superset of the property visibility to mark
     *                           it as hidden for only a subset of authorizations.
     * @param authorizations     The authorizations used.
     */
    default void markPropertyHidden(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        Iterable<Property> properties = getProperties(key, name);
        for (Property property : properties) {
            if (property.getVisibility().equals(propertyVisibility)) {
                markPropertyHidden(property, timestamp, visibility, authorizations);
                return;
            }
        }
        throw new IllegalArgumentException("Could not find property " + key + " : " + name + " : " + propertyVisibility);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param property       The property.
     * @param visibility     The visibility string under which this property is hidden.
     *                       This visibility can be a superset of the property visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     */
    default void markPropertyHidden(Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(property, null, visibility, authorizations);
    }

    /**
     * Marks a property as hidden for a given visibility.
     *
     * @param property       The property.
     * @param timestamp      The timestamp.
     * @param visibility     The visibility string under which this property is hidden.
     *                       This visibility can be a superset of the property visibility to mark
     *                       it as hidden for only a subset of authorizations.
     * @param authorizations The authorizations used.
     */
    void markPropertyHidden(Property property, Long timestamp, Visibility visibility, Authorizations authorizations);

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param visibility         The visibility string under which this property is now visible.
     * @param authorizations     The authorizations used.
     */
    default void markPropertyVisible(String key, String name, Visibility propertyVisibility, Visibility visibility, Authorizations authorizations) {
        markPropertyVisible(key, name, propertyVisibility, null, visibility, authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param key                The key of the property.
     * @param name               The name of the property.
     * @param propertyVisibility The visibility of the property.
     * @param timestamp          The timestamp.
     * @param visibility         The visibility string under which this property is now visible.
     * @param authorizations     The authorizations used.
     */
    default void markPropertyVisible(String key, String name, Visibility propertyVisibility, Long timestamp, Visibility visibility, Authorizations authorizations) {
        Iterable<Property> properties = getProperties(key, name);
        for (Property property : properties) {
            if (property.getVisibility().equals(propertyVisibility)) {
                markPropertyVisible(property, timestamp, visibility, authorizations);
                return;
            }
        }
        throw new IllegalArgumentException("Could not find property " + key + " : " + name + " : " + propertyVisibility);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param property       The property.
     * @param visibility     The visibility string under which this property is now visible.
     * @param authorizations The authorizations used.
     */
    default void markPropertyVisible(Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyVisible(property, null, visibility, authorizations);
    }

    /**
     * Marks a property as visible for a given visibility, effectively undoing markPropertyHidden.
     *
     * @param property       The property.
     * @param timestamp      The timestamp.
     * @param visibility     The visibility string under which this property is now visible.
     * @param authorizations The authorizations used.
     */
    void markPropertyVisible(Property property, Long timestamp, Visibility visibility, Authorizations authorizations);

    /**
     * Given the supplied authorizations is this element hidden?
     *
     * @param authorizations the authorizations to check against.
     * @return true, if it would be hidden from those authorizations.
     */
    default boolean isHidden(Authorizations authorizations) {
        for (Visibility visibility : getHiddenVisibilities()) {
            if (authorizations.canRead(visibility)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the list of hidden visibilities
     */
    Iterable<Visibility> getHiddenVisibilities();

    /**
     * Gets the extended data table names.
     */
    ImmutableSet<String> getExtendedDataTableNames();

    /**
     * Gets all the rows from an extended data table.
     *
     * @param tableName The name of the table to get rows from.
     * @return Iterable of all the rows.
     */
    default QueryableIterable<ExtendedDataRow> getExtendedData(String tableName) {
        return getExtendedData(tableName, getFetchHints());
    }

    /**
     * Gets all the rows from an extended data table.
     *
     * @param tableName The name of the table to get rows from.
     * @return Iterable of all the rows.
     */
    QueryableIterable<ExtendedDataRow> getExtendedData(String tableName, FetchHints fetchHints);

    /**
     * Gets all the rows from all the extended data tables.
     *
     * @return Iterable of all the rows.
     */
    default QueryableIterable<ExtendedDataRow> getExtendedData() {
        return getExtendedData(getFetchHints());
    }

    /**
     * Gets all the rows from all the extended data tables.
     *
     * @param fetchHints Fetch hints to filter extended data rows
     * @return Iterable of all the rows.
     */
    default QueryableIterable<ExtendedDataRow> getExtendedData(FetchHints fetchHints) {
        return getExtendedData(null, fetchHints);
    }

    /**
     * Fetch hints used when fetching this element.
     */
    FetchHints getFetchHints();

    @Override
    default int compareTo(Object o) {
        if (getClass().isInstance(o)) {
            return getId().compareTo(((Element) o).getId());
        }
        throw new ClassCastException("o must be an " + getClass().getName());
    }
}
