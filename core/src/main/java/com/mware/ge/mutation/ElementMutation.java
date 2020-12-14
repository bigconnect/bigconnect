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
import com.mware.ge.search.IndexHint;
import com.mware.ge.values.storable.Value;

public interface ElementMutation<T extends Element> extends ElementLocation {
    String DEFAULT_KEY = "";

    /**
     * saves the properties to the graph.
     *
     * @return the element which was mutated.
     */
    T save(Authorizations authorizations);

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
    ElementMutation<T> setProperty(String name, Value value, Visibility visibility);

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
    ElementMutation<T> setProperty(String name, Value value, Metadata metadata, Visibility visibility);

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param visibility The visibility to give this property.
     */
    ElementMutation<T> addPropertyValue(String key, String name, Value value, Visibility visibility);

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param visibility The visibility to give this property.
     */
    ElementMutation<T> addPropertyValue(String key, String name, Value value, Metadata metadata, Visibility visibility);

    /**
     * Adds or updates a property.
     *
     * @param key        The unique key given to the property allowing for multi-valued properties.
     * @param name       The name of the property.
     * @param value      The value of the property.
     * @param metadata   The metadata to assign to this property.
     * @param timestamp  The timestamp of the property.
     * @param visibility The visibility to give this property.
     */
    ElementMutation<T> addPropertyValue(String key, String name, Value value, Metadata metadata, Long timestamp, Visibility visibility);

    /**
     * Deletes a property.
     *
     * @param property the property to delete.
     */
    ElementMutation<T> deleteProperty(Property property);

    /**
     * Soft deletes a property.
     *
     * @param property the property to soft delete.
     */
    ElementMutation<T> softDeleteProperty(Property property);

    /**
     * Deletes the default property with that name.
     *
     * @param name       the property name to delete.
     * @param visibility the visibility of the property to delete.
     */
    ElementMutation<T> deleteProperty(String name, Visibility visibility);

    /**
     * Soft deletes the default property with that name.
     *
     * @param name       the property name to soft delete.
     * @param visibility the visibility of the property to soft delete.
     */
    ElementMutation<T> softDeleteProperty(String name, Visibility visibility);

    /**
     * Deletes a property.
     *
     * @param key        the key of the property to delete.
     * @param name       the name of the property to delete.
     * @param visibility the visibility of the property to delete.
     */
    ElementMutation<T> deleteProperty(String key, String name, Visibility visibility);

    /**
     * Soft deletes a property.
     *
     * @param key        the key of the property to soft delete.
     * @param name       the name of the property to soft delete.
     * @param visibility the visibility of the property to soft delete.
     */
    ElementMutation<T> softDeleteProperty(String key, String name, Visibility visibility);

    /**
     * Gets the properties currently in this mutation.
     */
    Iterable<Property> getProperties();

    /**
     * Gets the properties currently being deleted in this mutation.
     */
    Iterable<PropertyDeleteMutation> getPropertyDeletes();

    /**
     * Gets the properties currently being soft deleted in this mutation.
     */
    Iterable<PropertySoftDeleteMutation> getPropertySoftDeletes();

    /**
     * Gets the extended data mutations.
     */
    Iterable<ExtendedDataMutation> getExtendedData();

    /**
     * Gets the extended data columns currently being deleted in this mutation
     */
    Iterable<ExtendedDataDeleteMutation> getExtendedDataDeletes();

    /**
     * Sets the index hint of this element.
     */
    ElementMutation<T> setIndexHint(IndexHint indexHint);

    /**
     * Gets the currently set index hint.
     */
    IndexHint getIndexHint();

    /**
     * true, if this mutation has any changes. false, if this mutation is empty.
     */
    boolean hasChanges();

    /**
     * Adds an extended data cell to the element.
     *
     * @param tableName  The extended data table to add the cell to.
     * @param row        The row to add the cell to.
     * @param column     The column name.
     * @param value      The cell value.
     * @param visibility The visibility of the value.
     */
    ElementMutation<T> addExtendedData(String tableName, String row, String column, Value value, Visibility visibility);

    /**
     * Adds an extended data cell to the element.
     *
     * @param tableName  The extended data table to add the cell to.
     * @param row        The row to add the cell to.
     * @param column     The column name.
     * @param value      The cell value.
     * @param timestamp  The timestamp of the value. null, to automatically generate one.
     * @param visibility The visibility of the value.
     */
    ElementMutation<T> addExtendedData(String tableName, String row, String column, Value value, Long timestamp, Visibility visibility);

    /**
     * Adds an extended data cell to the element.
     *
     * @param tableName  The extended data table to add the cell to.
     * @param row        The row to add the cell to.
     * @param column     The column name.
     * @param key        The column multi-value key.
     * @param value      The cell value.
     * @param visibility The visibility of the value.
     */
    ElementMutation<T> addExtendedData(String tableName, String row, String column, String key, Value value, Visibility visibility);
    /**
     * Adds an extended data cell to the element.
     *
     * @param tableName  The extended data table to add the cell to.
     * @param row        The row to add the cell to.
     * @param column     The column name.
     * @param key        The column multi-value key.
     * @param value      The cell value.
     * @param timestamp  The timestamp of the value. null, to automatically generate one.
     * @param visibility The visibility of the value.
     */
    ElementMutation<T> addExtendedData(String tableName, String row, String column, String key, Value value, Long timestamp, Visibility visibility);

    /**
     * Deletes an extended data cell from an element.
     *
     * @param tableName  The extended data table to add the cell to.
     * @param row        The row to add the cell to.
     * @param column     The column name.
     * @param visibility The visibility of the value.
     */
    ElementMutation<T> deleteExtendedData(String tableName, String row, String column, Visibility visibility);

    /**
     * Deletes an extended data row from an element.
     *
     * @param tableName  The extended data table to add the cell to.
     * @param row        The row to add the cell to.
     */
    ElementMutation<T> deleteExtendedData(String tableName, String row);

    /**
     * Deletes an extended data cell from an element.
     *
     * @param tableName  The extended data table to add the cell to.
     * @param row        The row to add the cell to.
     * @param column     The column name.
     * @param key        The column multi-value key.
     * @param visibility The visibility of the value.
     */
    ElementMutation<T> deleteExtendedData(String tableName, String row, String column, String key, Visibility visibility);

    /**
     * Deletes an extended data table and its rows from an element.
     * @param tableName  The extended data table to delete.
    */
    ElementMutation<T> deleteExtendedDataTable(String tableName);

    ElementType getElementType();
}
