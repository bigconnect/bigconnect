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
package com.mware.ge.query;

import com.mware.ge.GeNotSupportedException;
import com.mware.ge.Property;
import com.mware.ge.PropertyDefinition;
import com.mware.ge.type.GeoShape;
import com.mware.ge.util.StreamUtils;
import com.mware.ge.values.storable.GeoShapeValue;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

public enum GeoCompare implements Predicate {
    INTERSECTS("intersects"),
    DISJOINT("disjoint"),
    WITHIN("within"),
    CONTAINS("contains");

    private final String compareName;

    GeoCompare(String compareName) {
        this.compareName = compareName;
    }

    public String getCompareName() {
        return compareName;
    }

    @Override
    public boolean evaluate(Iterable<Property> properties, Object second, Collection<PropertyDefinition> propertyDefinitions) {
        switch (this) {
            case WITHIN:
                AtomicBoolean hasProperty = new AtomicBoolean(false);
                boolean allMatch = StreamUtils.stream(properties).allMatch(property -> {
                    hasProperty.set(true);
                    return evaluate(property.getValue(), second);
                });
                return hasProperty.get() && allMatch;
            case INTERSECTS:
                return StreamUtils.stream(properties).anyMatch(property -> evaluate(property.getValue(), second));
            case DISJOINT:
                return StreamUtils.stream(properties).noneMatch(property -> evaluate(property.getValue(), second));
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }

    @Override
    public boolean evaluate(Object first, Object second, PropertyDefinition propertyDefinition) {
        return evaluate(first, second);
    }

    @Override
    public void validate(PropertyDefinition propertyDefinition) {
        if (!GeoShapeValue.class.isAssignableFrom(propertyDefinition.getDataType())) {
            throw new GeNotSupportedException("GeoCompare predicates are not allowed for properties of type " + propertyDefinition.getDataType().getName());
        }
    }


    private boolean evaluate(Object testValue, Object second) {
        GeoShape g1 = ((GeoShapeValue) testValue).asObjectCopy();
        GeoShape g2 = ((GeoShapeValue) second).asObjectCopy();
        switch (this) {
            case WITHIN:
                return g1.within(g2);
            case INTERSECTS:
                return g2.intersects(g1);
            case DISJOINT:
                return !g2.intersects(g1);
            default:
                throw new IllegalArgumentException("Invalid compare: " + this);
        }
    }
}
