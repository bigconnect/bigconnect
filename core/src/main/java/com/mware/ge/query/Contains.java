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

import com.mware.ge.Property;
import com.mware.ge.PropertyDefinition;
import com.mware.ge.GeException;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.ArrayValue;
import com.mware.ge.values.storable.Value;

import java.util.Collection;

public enum Contains implements Predicate {
    IN, NOT_IN;

    @Override
    public boolean evaluate(Iterable<Property> properties, Object second, Collection<PropertyDefinition> propertyDefinitions) {
        if (IterableUtils.count(properties) == 0 && this == NOT_IN) {
            return true;
        }

        for (Property property : properties) {
            PropertyDefinition propertyDefinition = PropertyDefinition.findPropertyDefinition(propertyDefinitions, property.getName());
            if (evaluate(property.getValue(), second, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void validate(PropertyDefinition propertyDefinition) {
    }

    @Override
    public boolean evaluate(Object first, Object second, PropertyDefinition propertyDefinition) {
        if (second instanceof Iterable) {
            switch (this) {
                case IN:
                    return evaluateInIterable(first, (Iterable) second, propertyDefinition);
                case NOT_IN:
                    return !evaluateInIterable(first, (Iterable) second, propertyDefinition);
                default:
                    throw new GeException("Not implemented: " + this);
            }
        }

        if (second instanceof ArrayValue)
            second = ((ArrayValue)second).asObjectCopy();

        if (second.getClass().isArray()) {
            switch (this) {
                case IN:
                    return evaluateInIterable(first, (Object[]) second, propertyDefinition);
                case NOT_IN:
                    return !evaluateInIterable(first, (Object[]) second, propertyDefinition);
                default:
                    throw new GeException("Not implemented: " + this);
            }
        } else {
            switch (this) {
                case IN:
                    return evaluateInIterable(first, new Object[] { second }, propertyDefinition);
                case NOT_IN:
                    return !evaluateInIterable(first, new Object[] { second }, propertyDefinition);
                default:
                    throw new GeException("Not implemented: " + this);
            }
        }
    }

    private boolean evaluateInIterable(Object first, Iterable second, PropertyDefinition propertyDefinition) {
        for (Object o : second) {
            if (Compare.evaluate(first, Compare.EQUAL, o, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluateInIterable(Object first, Object[] second, PropertyDefinition propertyDefinition) {
        for (Object o : second) {
            if (Compare.evaluate(first, Compare.EQUAL, o, propertyDefinition)) {
                return true;
            }
        }
        return false;
    }
}
