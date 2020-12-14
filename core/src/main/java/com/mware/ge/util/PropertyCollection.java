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
package com.mware.ge.util;

import com.mware.ge.Property;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class PropertyCollection {
    private final ConcurrentSkipListSet<Property> propertiesList = new ConcurrentSkipListSet<>();
    private final Map<String, ConcurrentSkipListMap<String, ConcurrentSkipListSet<Property>>> propertiesByNameAndKey = new HashMap<>();

    public Iterable<Property> getProperties() {
        return propertiesList;
    }

    public synchronized Iterable<Property> getProperties(String key, String name) {
        if (key == null) {
            return getProperties(name);
        }

        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey == null) {
            return new ArrayList<>();
        }
        ConcurrentSkipListSet<Property> properties = propertiesByKey.get(key);
        if (properties == null) {
            return new ArrayList<>();
        }
        return properties;
    }

    public synchronized Iterable<Property> getProperties(String name) {
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey == null) {
            return new ArrayList<>();
        }
        List<Property> results = new ArrayList<>();
        for (ConcurrentSkipListSet<Property> properties : propertiesByKey.values()) {
            results.addAll(properties);
        }
        return results;
    }

    public synchronized Property getProperty(String name, int index) {
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey == null) {
            return null;
        }
        for (ConcurrentSkipListSet<Property> properties : propertiesByKey.values()) {
            for (Property property : properties) {
                if (index == 0) {
                    return property;
                }
                index--;
            }
        }
        return null;
    }

    public synchronized Property getProperty(String key, String name, int index) {
        if (key == null) {
            return getProperty(name, index);
        }
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey == null) {
            return null;
        }
        ConcurrentSkipListSet<Property> properties = propertiesByKey.get(key);
        if (properties == null) {
            return null;
        }
        for (Property property : properties) {
            if (index == 0) {
                return property;
            }
            index--;
        }
        return null;
    }

    public synchronized void addProperty(Property property) {
        ConcurrentSkipListMap<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(property.getName());
        if (propertiesByKey == null) {
            propertiesByKey = new ConcurrentSkipListMap<>();
            this.propertiesByNameAndKey.put(property.getName(), propertiesByKey);
        }

        if (null == property.getKey()) {
            return;
        }
        ConcurrentSkipListSet<Property> properties = propertiesByKey.get(property.getKey());
        if (properties == null) {
            properties = new ConcurrentSkipListSet<>();
            propertiesByKey.put(property.getKey(), properties);
        }
        properties.add(property);
        this.propertiesList.add(property);
    }

    public synchronized void removeProperty(Property property) {
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(property.getName());
        if (propertiesByKey == null) {
            return;
        }

        if (null == property.getKey()) {
            return;
        }
        ConcurrentSkipListSet<Property> properties = propertiesByKey.get(property.getKey());
        if (properties == null) {
            return;
        }
        properties.remove(property);
        this.propertiesList.remove(property);
    }

    public synchronized Iterable<Property> removeProperties(String name) {
        List<Property> removedProperties = new ArrayList<>();
        Map<String, ConcurrentSkipListSet<Property>> propertiesByKey = propertiesByNameAndKey.get(name);
        if (propertiesByKey != null) {
            for (ConcurrentSkipListSet<Property> properties : propertiesByKey.values()) {
                for (Property property : properties) {
                    removedProperties.add(property);
                }
            }
        }

        for (Property property : removedProperties) {
            removeProperty(property);
        }

        return removedProperties;
    }
}
