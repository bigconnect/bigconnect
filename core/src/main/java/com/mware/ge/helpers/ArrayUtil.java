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
/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.ge.helpers;

import static java.util.Arrays.copyOf;

public abstract class ArrayUtil {
    /**
     * Count items from a different array contained in an array.
     * The order of items doesn't matter.
     *
     * @param array    Array to examine
     * @param contains Items to look for
     * @param <T>      The type of the array items
     * @return {@code true} if all items in {@code contains} exists in {@code array}, otherwise {@code false}.
     */
    @Deprecated
    public static <T> boolean containsAll(T[] array, T[] contains) {
        for (T check : contains) {
            if (!contains(array, check)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check if array contains item.
     *
     * @param array    Array to examine
     * @param contains Single item to look for
     * @param <T>      The type of the array items
     * @return {@code true} if {@code contains} exists in {@code array}, otherwise {@code false}.
     */
    @Deprecated
    public static <T> boolean contains(T[] array, T contains) {
        return contains(array, array.length, contains);
    }

    /**
     * Check if array contains item.
     *
     * @param array       Array to examine
     * @param arrayLength Number of items to check, from the start of the array
     * @param contains    Single item to look for
     * @param <T>         The type of the array items
     * @return {@code true} if {@code contains} exists in {@code array}, otherwise {@code false}.
     */
    @Deprecated
    public static <T> boolean contains(T[] array, int arrayLength, T contains) {
        for (int i = 0; i < arrayLength; i++) {
            T item = array[i];
            if (nullSafeEquals(item, contains)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compare two items for equality; if both are {@code null} they are regarded as equal.
     *
     * @param first First item to compare
     * @param other Other item to compare
     * @param <T>   The type of the items
     * @return {@code true} if {@code first} and {@code other} are both {@code null} or are both equal.
     */
    @Deprecated
    public static <T> boolean nullSafeEquals(T first, T other) {
        return first == null ? first == other : first.equals(other);
    }


    /**
     * Get the union of two arrays.
     * The resulting array will not contain any duplicates.
     *
     * @param first First array
     * @param other Other array
     * @param <T>   The type of the arrays
     * @return an array containing the union of {@code first} and {@code other}. Items occurring in
     * both {@code first} and {@code other} will only have of the two in the resulting union.
     */
    @Deprecated
    public static <T> T[] union(T[] first, T[] other) {
        if (first == null || other == null) {
            return first == null ? other : first;
        }

        int missing = missing(first, other);
        if (missing == 0) {
            return first;
        }

        // An attempt to add the labels as efficiently as possible
        T[] union = copyOf(first, first.length + missing);
        int cursor = first.length;
        for (T candidate : other) {
            if (!contains(first, candidate)) {
                union[cursor++] = candidate;
                missing--;
            }
        }
        assert missing == 0;
        return union;
    }

    /**
     * Count missing items in an array.
     * The order of items doesn't matter.
     *
     * @param array    Array to examine
     * @param contains Items to look for
     * @param <T>      The type of the array items
     * @return how many of the items in {@code contains} are missing from {@code array}.
     */
    @Deprecated
    public static <T> int missing(T[] array, T[] contains) {
        int missing = 0;
        for (T check : contains) {
            if (!contains(array, check)) {
                missing++;
            }
        }
        return missing;
    }

    private ArrayUtil() {   // No instances allowed
    }
}
