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

import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.values.storable.Value;

import java.util.Iterator;

public interface GeObject extends Comparable {
    /**
     * Id of the object
     */
    Object getId();

    /**
     * an Iterable of all the properties on this element that you have access to based on the authorizations
     * used to retrieve the element.
     */
    Iterable<Property> getProperties();

    /**
     * Gets a property by name. This assumes a single valued property. If multiple property values exists this will only return the first one.
     *
     * @param name the name of the property.
     * @return The property if found. null, if not found.
     */
    default Property getProperty(String name) {
        Iterator<Property> propertiesWithName = getProperties(name).iterator();
        if (propertiesWithName.hasNext()) {
            return propertiesWithName.next();
        }
        return null;
    }

    /**
     * Convenience method to retrieve the first value of the property with the given name. This method calls
     * Element#getPropertyValue(java.lang.String, int) with an index of 0.
     * <p></p>
     * This method makes no attempt to verify that one and only one value exists given the name.
     *
     * @param name The name of the property to retrieve
     * @return The value of the property. null, if the property was not found.
     */
    default Value getPropertyValue(String name) {
        return getPropertyValue(name, 0);
    }

    /**
     * Gets a property by key and name.
     *
     * @param key  the key of the property.
     * @param name the name of the property.
     * @return The property if found. null, if not found.
     */
    default Property getProperty(String key, String name) {
        return getProperty(key, name, null);
    }

    /**
     * Gets a property by key, name, and visibility.
     *
     * @param key        the key of the property.
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     * @return The property if found. null, if not found.
     */
    Property getProperty(String key, String name, Visibility visibility);

    /**
     * Gets a property by name, and visibility.
     *
     * @param name       the name of the property.
     * @param visibility The visibility of the property to get.
     * @return The property if found. null, if not found.
     */
    Property getProperty(String name, Visibility visibility);

    /**
     * an Iterable of all the properties with the given name on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param name The name of the property to retrieve
     */
    Iterable<Property> getProperties(String name);

    /**
     * an Iterable of all the properties with the given name and key on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param key  The property key
     * @param name The name of the property to retrieve
     */
    Iterable<Property> getProperties(String key, String name);

    /**
     * an Iterable of all the property values with the given name on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param name The name of the property to retrieve
     */
    default Iterable<Value> getPropertyValues(String name) {
        return new ConvertingIterable<Property, Value>(getProperties(name)) {
            @Override
            protected Value convert(Property o) {
                return o.getValue();
            }
        };
    }

    /**
     * an Iterable of all the property values with the given name and key on this element that you have access to based on the authorizations
     * used to retrieve the element.
     *
     * @param key  The property key
     * @param name The name of the property to retrieve
     */
    default Iterable<Value> getPropertyValues(String key, String name) {
        return new ConvertingIterable<Property, Value>(getProperties(key, name)) {
            @Override
            protected Value convert(Property p) {
                return p.getValue();
            }
        };
    }

    /**
     * Convenience method to retrieve the first value of the property with the given name. This method calls
     * Element#getPropertyValue(java.lang.String, java.lang.String, int) with an index of 0.
     * <p></p>
     * This method makes no attempt to verify that one and only one value exists given the name.
     *
     * @param key  The key of the property
     * @param name The name of the property to retrieve
     * @return The value of the property. null, if the property was not found.
     */
    default Value getPropertyValue(String key, String name) {
        return getPropertyValue(key, name, 0);
    }

    /**
     * Gets the nth property value of the named property. If the named property has multiple values this method
     * provides an easy way to get the value by index.
     * <p></p>
     * This method is a convenience method and calls Element#getPropertyValues(java.lang.String)
     * and iterates over that list until the nth value.
     * <p></p>
     * This method assumes the property values are retrieved in a deterministic order.
     *
     * @param name  The name of the property to retrieve.
     * @param index The zero based index into the values.
     * @return The value of the property. null, if the property doesn't exist or doesn't have that many values.
     */
    default Value getPropertyValue(String name, int index) {
        Iterator<Value> values = getPropertyValues(name).iterator();
        while (values.hasNext() && index >= 0) {
            Value v = values.next();
            if (index == 0) {
                return v;
            }
            index--;
        }
        return null;
    }

    /**
     * Gets the nth property value of the named property. If the named property has multiple values this method
     * provides an easy way to get the value by index.
     * <p></p>
     * This method is a convenience method and calls Element#getPropertyValues(java.lang.String, java.lang.String)
     * and iterates over that list until the nth value.
     * <p></p>
     * This method assumes the property values are retrieved in a deterministic order.
     *
     * @param key   The property key
     * @param name  The name of the property to retrieve.
     * @param index The zero based index into the values.
     * @return The value of the property. null, if the property doesn't exist or doesn't have that many values.
     */
    default Value getPropertyValue(String key, String name, int index) {
        Iterator<Value> values = getPropertyValues(key, name).iterator();
        while (values.hasNext() && index >= 0) {
            Value v = values.next();
            if (index == 0) {
                return v;
            }
            index--;
        }
        return null;
    }
}
