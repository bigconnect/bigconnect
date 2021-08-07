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

import com.mware.ge.values.AnyValues;
import com.mware.ge.values.storable.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

public class ObjectUtils {
    public static boolean equals(Object leftObj, Object rightObj) {
        return compare(leftObj, rightObj) == 0;
    }

    @SuppressWarnings("unchecked")
    public static int compare(Object first, Object second) {
        if (first == null && second == null) {
            return 0;
        }

        if (first == null) {
            return 1;
        }
        if (second == null) {
            return -1;
        }

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

        if (first instanceof TextValue)
            first = ((TextValue)first).toLower().asObjectCopy();

        if (second instanceof TextValue)
            second = ((TextValue)second).toLower().asObjectCopy();

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
            } catch (NumberFormatException ex) {
                return -1;
            }
        }
        if (first instanceof String && second instanceof Number) {
            try {
                double firstDouble = Double.parseDouble(first.toString());
                double secondDouble = ((Number) second).doubleValue();
                return Double.compare(firstDouble, secondDouble);
            } catch (NumberFormatException ex) {
                return 1;
            }
        }

        if (first instanceof EdgeVertexIds && second instanceof String)
            return ((EdgeVertexIds)first).compareTo((String) second);

        if (first instanceof Comparable) {
            return ((Comparable) first).compareTo(second);
        }
        if (second instanceof Comparable) {
            return ((Comparable) second).compareTo(first);
        }
        if (first instanceof Value && second instanceof Value) {
            return AnyValues.COMPARATOR.compare((Value)first, (Value)second);
        }
        return first.equals(second) ? 0 : 1;
    }

    private static int compareCollections(Collection leftObj, Collection rightObj) {
        int sizeCompare = Integer.compare(leftObj.size(), rightObj.size());
        if (sizeCompare != 0) {
            return sizeCompare;
        }

        Iterator leftIt = leftObj.iterator();
        Iterator rightIt = rightObj.iterator();
        while (leftIt.hasNext() && rightIt.hasNext()) {
            int c = compare(leftIt.next(), rightIt.next());
            if (c != 0) {
                return c;
            }
        }

        return 0;
    }

    private static int compareStreams(Stream leftObj, Stream rightObj) {
        Iterator leftIt = leftObj.iterator();
        Iterator rightIt = rightObj.iterator();
        while (leftIt.hasNext() && rightIt.hasNext()) {
            int c = compare(leftIt.next(), rightIt.next());
            if (c != 0) {
                return c;
            }
        }

        if (leftIt.hasNext()) {
            return -1;
        }
        if (rightIt.hasNext()) {
            return 1;
        }
        return 0;
    }
}
