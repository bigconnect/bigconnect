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
package com.mware.ge.collection;

/**
 * Utility to handle pairs of objects.
 *
 * @param <T1> the type of the {@link #first() first value} of the pair.
 * @param <T2> the type of the {@link #other() other value} of the pair.
 */
public abstract class Pair<T1, T2> {
    @SuppressWarnings("rawtypes")
    private static final Pair EMPTY = Pair.of(null, null);

    @SuppressWarnings("unchecked")
    public static <T1, T2> Pair<T1, T2> empty() {
        return EMPTY;
    }

    /**
     * Create a new pair of objects.
     *
     * @param first the first object in the pair.
     * @param other the other object in the pair.
     * @param <T1>  the type of the first object in the pair
     * @param <T2>  the type of the second object in the pair
     * @return a new pair of the two parameters.
     */
    public static <T1, T2> Pair<T1, T2> pair(final T1 first, final T2 other) {
        return new Pair<T1, T2>() {
            @Override
            public T1 first() {
                return first;
            }

            @Override
            public T2 other() {
                return other;
            }
        };
    }

    /**
     * Alias of {@link #pair(Object, Object)}.
     *
     * @param first the first object in the pair.
     * @param other the other object in the pair.
     * @param <T1>  the type of the first object in the pair
     * @param <T2>  the type of the second object in the pair
     * @return a new pair of the two parameters.
     */
    public static <T1, T2> Pair<T1, T2> of(final T1 first, final T2 other) {
        return pair(first, other);
    }

    Pair() {
        // package private, limited number of subclasses
    }

    /**
     * @return the first object in the pair.
     */
    public abstract T1 first();

    /**
     * @return the other object in the pair.
     */
    public abstract T2 other();

    @Override
    public String toString() {
        return "(" + first() + ", " + other() + ")";
    }

    @Override
    public int hashCode() {
        return (31 * hashCode(first())) | hashCode(other());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Pair) {
            @SuppressWarnings("rawtypes")
            Pair that = (Pair) obj;
            return equals(this.other(), that.other()) && equals(this.first(), that.first());
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
