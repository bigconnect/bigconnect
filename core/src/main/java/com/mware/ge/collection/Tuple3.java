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
package com.mware.ge.collection;

public abstract class Tuple3<T1, T2, T3> {
    Tuple3() {
        // package private, limited number of subclasses
    }

    public static <T1, T2, T3> Tuple3<T1, T2, T3> of(final T1 first, final T2 second, final T3 third) {
        return new Tuple3<T1, T2, T3>() {
            @Override
            public T1 first() {
                return first;
            }

            @Override
            public T2 second() {
                return second;
            }

            @Override
            public T3 third() {
                return third;
            }
        };
    }

    public abstract T1 first();
    public abstract T2 second();
    public abstract T3 third();

    @Override
    public int hashCode() {
        return (31 * hashCode(first())) | hashCode(second()) | hashCode(third());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Tuple3) {
            @SuppressWarnings("rawtypes")
            Tuple3 that = (Tuple3) obj;
            return equals(this.third(), that.third())
                    && equals(this.second(), that.second())
                    && equals(this.first(), that.first());
        }
        return false;
    }

    static int hashCode(Object obj) {
        return obj == null ? 0 : obj.hashCode();
    }

    static boolean equals(Object obj1, Object obj2) {
        return (obj1 == obj2) || (obj1 != null && obj1.equals(obj2));
    }
}
