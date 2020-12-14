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
package com.mware.ge.values.storable;

import com.mware.ge.hashing.HashFunction;

import java.util.Arrays;

public abstract class NonPrimitiveArray<T extends Comparable<? super T>> extends ArrayValue {
    protected abstract T[] value();

    @Override
    public final boolean equals(boolean[] x) {
        return false;
    }

    @Override
    public final boolean equals(char[] x) {
        return false;
    }

    @Override
    public final boolean equals(String[] x) {
        return false;
    }

    @Override
    public final boolean equals(byte[] x) {
        return false;
    }

    @Override
    public final boolean equals(short[] x) {
        return false;
    }

    @Override
    public final boolean equals(int[] x) {
        return false;
    }

    @Override
    public final boolean equals(long[] x) {
        return false;
    }

    @Override
    public final boolean equals(float[] x) {
        return false;
    }

    @Override
    public final boolean equals(double[] x) {
        return false;
    }

    @Override
    public final NumberType numberType() {
        return NumberType.NO_NUMBER;
    }

    protected final int compareToNonPrimitiveArray(NonPrimitiveArray<T> other) {
        int i = 0;
        int x = 0;
        int length = Math.min(this.length(), other.length());

        while (x == 0 && i < length) {
            x = this.value()[i].compareTo(other.value()[i]);
            i++;
        }
        if (x == 0) {
            x = this.length() - other.length();
        }
        return x;
    }

    @Override
    public final int computeHash() {
        return Arrays.hashCode(value());
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        hash = hashFunction.update(hash, length());
        for (T obj : value()) {
            hash = hashFunction.update(hash, obj.hashCode());
        }
        return hash;
    }

    @Override
    public final int length() {
        return value().length;
    }

    @Override
    public final T[] asObjectCopy() {
        return value().clone();
    }

    @Override
    @Deprecated
    public final T[] asObject() {
        return value();
    }

    @Override
    public final String prettyPrint() {
        return Arrays.toString(value());
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + Arrays.toString(value());
    }
}
