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
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.utils.ValueMath;

public abstract class IntegralValue extends NumberValue {
    public static long safeCastIntegral(String name, AnyValue value, long defaultValue) {
        if (value == null || value == Values.NO_VALUE) {
            return defaultValue;
        }
        if (value instanceof IntegralValue) {
            return ((IntegralValue) value).longValue();
        }
        throw new IllegalArgumentException(
                name + " must be an integer value, but was a " + value.getClass().getSimpleName());
    }

    @Override
    public boolean equals(long x) {
        return longValue() == x;
    }

    @Override
    public boolean equals(double x) {
        return NumberValues.numbersEqual(x, longValue());
    }

    @Override
    public final int computeHash() {
        return NumberValues.hash(longValue());
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        return hashFunction.update(hash, longValue());
    }

    @Override
    public boolean eq(Object other) {
        return other instanceof Value && equals((Value) other);
    }

    @Override
    public final boolean equals(Value other) {
        if (other instanceof IntegralValue) {
            IntegralValue that = (IntegralValue) other;
            return this.longValue() == that.longValue();
        } else if (other instanceof FloatingPointValue) {
            FloatingPointValue that = (FloatingPointValue) other;
            return NumberValues.numbersEqual(that.doubleValue(), this.longValue());
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(IntegralValue other) {
        return Long.compare(longValue(), other.longValue());
    }

    @Override
    public int compareTo(FloatingPointValue other) {
        return NumberValues.compareLongAgainstDouble(longValue(), other.doubleValue());
    }

    @Override
    public NumberType numberType() {
        return NumberType.INTEGRAL;
    }

    @Override
    public double doubleValue() {
        return longValue();
    }

    @Override
    public LongValue minus(long b) {
        return ValueMath.subtract(longValue(), b);
    }

    @Override
    public DoubleValue minus(double b) {
        return ValueMath.subtract(longValue(), b);
    }

    @Override
    public LongValue plus(long b) {
        return ValueMath.add(longValue(), b);
    }

    @Override
    public DoubleValue plus(double b) {
        return ValueMath.add(longValue(), b);
    }

    @Override
    public LongValue times(long b) {
        return ValueMath.multiply(longValue(), b);
    }

    @Override
    public DoubleValue times(double b) {
        return ValueMath.multiply(longValue(), b);
    }

    @Override
    public LongValue dividedBy(long b) {
        return Values.longValue(longValue() / b);
    }

    @Override
    public DoubleValue dividedBy(double b) {
        return Values.doubleValue(doubleValue() / b);
    }
}
