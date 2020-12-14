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

import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.Value;

import java.util.List;

public interface ExistingElementMutation<T extends Element> extends ElementMutation<T> {
    /**
     * Alters the visibility of a property.
     *
     * @param property   The property to mutate.
     * @param visibility The new visibility.
     */
    ExistingElementMutation<T> alterPropertyVisibility(Property property, Visibility visibility);

    /**
     * Alters the visibility of a property.
     *
     * @param key        The key of a multivalued property.
     * @param name       The name of the property to alter the visibility of.
     * @param visibility The new visibility.
     */
    ExistingElementMutation<T> alterPropertyVisibility(String key, String name, Visibility visibility);

    /**
     * Alters the visibility of a property.
     *
     * @param name       The name of the property to alter the visibility of.
     * @param visibility The new visibility.
     */
    ExistingElementMutation<T> alterPropertyVisibility(String name, Visibility visibility);

    /**
     * Gets the properties whose visibilities are being altered in this mutation.
     */
    List<AlterPropertyVisibility> getAlterPropertyVisibilities();

    /**
     * Alters the visibility of the element (vertex or edge).
     *
     * @param visibility The new visibility.
     */
    ExistingElementMutation<T> alterElementVisibility(Visibility visibility);

    /**
     * Get the new element visibility or null if not being altered in this mutation.
     */
    Visibility getNewElementVisibility();
    /**
     * Get the old element visibility.
     */
    Visibility getOldElementVisibility();

    /**
     * Sets a property metadata value on a property.
     *
     * @param property     The property to mutate.
     * @param metadataName The name of the metadata.
     * @param newValue     The new value.
     * @param visibility   The visibility of the metadata item
     */
    ExistingElementMutation<T> setPropertyMetadata(Property property, String metadataName, Value newValue, Visibility visibility);

    /**
     * Sets a property metadata value on a property.
     *
     * @param propertyKey  The key of a multivalued property.
     * @param propertyName The name of the property.
     * @param metadataName The name of the metadata.
     * @param newValue     The new value.
     * @param visibility   The visibility of the metadata item
     */
    ExistingElementMutation<T> setPropertyMetadata(String propertyKey, String propertyName, String metadataName, Value newValue, Visibility visibility);

    /**
     * Sets a property metadata value on a property.
     *
     * @param propertyName The name of the property.
     * @param metadataName The name of the metadata.
     * @param newValue     The new value.
     * @param visibility   The visibility of the metadata item
     */
    ExistingElementMutation<T> setPropertyMetadata(String propertyName, String metadataName, Value newValue, Visibility visibility);

    /**
     * Gets all of the property metadata changes that are part of this mutation.
     */
    List<SetPropertyMetadata> getSetPropertyMetadatas();

    /**
     * Permanently deletes all default properties with that name irregardless of visibility.
     *
     * @param name the property name to delete.
     */
    ExistingElementMutation<T> deleteProperties(String name);

    /**
     * Permanently deletes all properties with this key and name irregardless of visibility.
     *
     * @param key  the key of the property to delete.
     * @param name the name of the property to delete.
     */
    ExistingElementMutation<T> deleteProperties(String key, String name);

    /**
     * Soft deletes all default properties with that name irregardless of visibility.
     *
     * @param name the property name to delete.
     */
    ExistingElementMutation<T> softDeleteProperties(String name);

    /**
     * Soft deletes all properties with this key and name irregardless of visibility.
     *
     * @param key  the key of the property to delete.
     * @param name the name of the property to delete.
     */
    ExistingElementMutation<T> softDeleteProperties(String key, String name);

    /**
     * Gets the element this mutation is affecting.
     *
     * @return The element.
     */
    T getElement();
}
