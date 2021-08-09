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

import com.mware.ge.*;
import com.mware.ge.collection.Iterables;
import com.mware.ge.util.ObjectUtils;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;

import java.util.Collection;

public enum Compare implements Predicate {
    EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL, STARTS_WITH, ENDS_WITH, RANGE;

    @Override
    public boolean evaluate(final Iterable<Property> properties, final Object second) {
        for (Property property : properties) {
            if (evaluate(property, second)) {
                return true;
            }
        }

        if (Iterables.count(properties) == 0 && NOT_EQUAL.equals(this)) {
            return true;
        }

        return false;
    }

    @Override
    public void validate() {
    }

    @Override
    public boolean evaluate(Object first, Object second) {
        Compare comparePredicate = this;
        return evaluate(first, comparePredicate, second);
    }

    private boolean evaluate(Property property, Object second) {
        Object first = property.getValue();
        Compare comparePredicate = this;

        return evaluate(first, comparePredicate, second);
    }

    static boolean evaluate(Object first, Compare comparePredicate, Object second) {
        if (first instanceof ElementType) {
            first = ((ElementType) first).name();
        }
        if (second instanceof ElementType) {
            second = ((ElementType) second).name();
        }

        switch (comparePredicate) {
            case EQUAL:
                if (null == first) {
                    return second == null;
                }
                return ObjectUtils.compare(first, second) == 0;
            case NOT_EQUAL:
                if (null == first) {
                    return second != null;
                }
                return ObjectUtils.compare(first, second) != 0;
            case GREATER_THAN:
                if (null == first || second == null) {
                    return false;
                }
                return ObjectUtils.compare(first, second) >= 1;
            case LESS_THAN:
                if (null == first || second == null) {
                    return false;
                }
                return ObjectUtils.compare(first, second) <= -1;
            case GREATER_THAN_EQUAL:
                if (null == first || second == null) {
                    return false;
                }
                return ObjectUtils.compare(first, second) >= 0;
            case LESS_THAN_EQUAL:
                if (null == first || second == null) {
                    return false;
                }
                return ObjectUtils.compare(first, second) <= 0;
            case STARTS_WITH:
                if (!(second instanceof TextValue)) {
                    throw new GeException("STARTS_WITH may only be used to query String values");
                }
                if (null == first) {
                    return second == null;
                }
                return ((TextValue)first).startsWith((TextValue) second);
            case ENDS_WITH:
                if (!(second instanceof TextValue)) {
                    throw new GeException("ENDS_WITH may only be used to query String values");
                }
                if (null == first) {
                    return second == null;
                }
                return ((TextValue)first).endsWith((TextValue) second);
            case RANGE:
                if (first instanceof Range) {
                    return ((Range) first).isInRange(second);
                } else if (second instanceof Range) {
                    return ((Range) second).isInRange(first);
                } else {
                    throw new IllegalArgumentException("Invalid range values: " + first + ", " + second);
                }
            default:
                throw new IllegalArgumentException("Invalid compare: " + comparePredicate);
        }
    }

    private static int compare(Object first, Object second) {
        if (first instanceof StreamingPropertyValue && TextValue.class.isAssignableFrom(((StreamingPropertyValue) first).getValueType())) {
            first = ((StreamingPropertyValue) first).readToString();
        }
        if (second instanceof StreamingPropertyValue && TextValue.class.isAssignableFrom(((StreamingPropertyValue) second).getValueType())) {
            second = ((StreamingPropertyValue) second).readToString();
        }

        if (first instanceof String) {
            first = ((String) first).toLowerCase();
        }
        if (second instanceof String) {
            second = ((String) second).toLowerCase();
        }

        if (first instanceof Long && second instanceof Long) {
            long firstLong = (long) first;
            long secondLong = (long) second;
            return Long.compare(firstLong, secondLong);
        }
        if (first instanceof Integer && second instanceof Integer) {
            int firstInt = (int) first;
            int secondInt = (int) second;
            return Integer.compare(firstInt, secondInt);
        }
        if (first instanceof Number && second instanceof Number) {
            double firstDouble = ((Number) first).doubleValue();
            double secondDouble = ((Number) second).doubleValue();
            return Double.compare(firstDouble, secondDouble);
        }
        if (first instanceof Number && second instanceof String) {
            try {
                double firstDouble = ((Number) first).doubleValue();
                double secondDouble = Double.parseDouble(second.toString());
                return Double.compare(firstDouble, secondDouble);
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        if (first instanceof String && second instanceof Number) {
            try {
                double firstDouble = Double.parseDouble(first.toString());
                double secondDouble = ((Number) second).doubleValue();
                return Double.compare(firstDouble, secondDouble);
            } catch (NumberFormatException e) {
                return 1;
            }
        }
        if (first instanceof Comparable) {
            return ((Comparable) first).compareTo(second);
        }
        if (second instanceof Comparable) {
            return ((Comparable) second).compareTo(first);
        }
        return first.equals(second) ? 0 : 1;
    }
}
