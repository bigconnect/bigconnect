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
package com.mware.ge.values.virtual;


import com.mware.ge.values.AnyValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.VirtualValue;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

final class ArrayHelpers {
    private ArrayHelpers() {
    }

    static boolean isSortedSet(int[] keys) {
        for (int i = 0; i < keys.length - 1; i++) {
            if (keys[i] >= keys[i + 1]) {
                return false;
            }
        }
        return true;
    }

    static boolean isSortedSet(VirtualValue[] keys, Comparator<AnyValue> comparator) {
        for (int i = 0; i < keys.length - 1; i++) {
            if (comparator.compare(keys[i], keys[i + 1]) >= 0) {
                return false;
            }
        }
        return true;
    }

    static boolean isSortedSet(Value[] keys, Comparator<AnyValue> comparator) {
        for (int i = 0; i < keys.length - 1; i++) {
            if (comparator.compare(keys[i], keys[i + 1]) >= 0) {
                return false;
            }
        }
        return true;
    }

    static boolean containsNull(AnyValue[] values) {
        for (AnyValue value : values) {
            if (value == null) {
                return true;
            }
        }
        return false;
    }

    static boolean containsNull(List<AnyValue> values) {
        for (AnyValue value : values) {
            if (value == null) {
                return true;
            }
        }
        return false;
    }

    static <T> Iterator<T> asIterator(T[] array) {
        assert array != null;
        return new Iterator<T>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < array.length;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return array[index++];
            }
        };
    }
}
