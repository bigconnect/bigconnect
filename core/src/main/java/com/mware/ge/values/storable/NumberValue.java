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

import com.mware.ge.values.AnyValue;

public abstract class NumberValue extends ScalarValue {
    public static double safeCastFloatingPoint(String name, AnyValue value, double defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof IntegralValue) {
            return ((IntegralValue) value).doubleValue();
        }
        if (value instanceof FloatingPointValue) {
            return ((FloatingPointValue) value).doubleValue();
        }
        throw new IllegalArgumentException(
                name + " must be a number value, but was a " + value.getClass().getSimpleName());
    }

    public abstract double doubleValue();

    public abstract long longValue();

    public abstract int compareTo(IntegralValue other);

    public abstract int compareTo(FloatingPointValue other);

    @Override
    int unsafeCompareTo(Value otherValue) {
        if (otherValue instanceof IntegralValue) {
            return compareTo((IntegralValue) otherValue);
        } else if (otherValue instanceof FloatingPointValue) {
            return compareTo((FloatingPointValue) otherValue);
        } else {
            throw new IllegalArgumentException("Cannot compare different values");
        }
    }

    @Override
    public abstract Number asObjectCopy();

    @Override
    public Number asObject() {
        return asObjectCopy();
    }

    @Override
    public final boolean equals(boolean x) {
        return false;
    }

    @Override
    public final boolean equals(char x) {
        return false;
    }

    @Override
    public final boolean equals(String x) {
        return false;
    }

    @Override
    public ValueGroup valueGroup() {
        return ValueGroup.NUMBER;
    }

    public abstract NumberValue minus(long b);

    public abstract NumberValue minus(double b);

    public abstract NumberValue plus(long b);

    public abstract NumberValue plus(double b);

    public abstract NumberValue times(long b);

    public abstract NumberValue times(double b);

    public abstract NumberValue dividedBy(long b);

    public abstract NumberValue dividedBy(double b);

    public NumberValue minus(NumberValue numberValue) {
        if (numberValue instanceof IntegralValue) {
            return minus(numberValue.longValue());
        } else if (numberValue instanceof FloatingPointValue) {
            return minus(numberValue.doubleValue());
        } else {
            throw new IllegalArgumentException("Cannot subtract " + numberValue);
        }
    }

    public NumberValue plus(NumberValue numberValue) {
        if (numberValue instanceof IntegralValue) {
            return plus(numberValue.longValue());
        } else if (numberValue instanceof FloatingPointValue) {
            return plus(numberValue.doubleValue());
        } else {
            throw new IllegalArgumentException("Cannot add " + numberValue);
        }
    }

    public NumberValue times(NumberValue numberValue) {
        if (numberValue instanceof IntegralValue) {
            return times(numberValue.longValue());
        } else if (numberValue instanceof FloatingPointValue) {
            return times(numberValue.doubleValue());
        } else {
            throw new IllegalArgumentException("Cannot multiply with " + numberValue);
        }
    }

    public NumberValue divideBy(NumberValue numberValue) {
        if (numberValue instanceof IntegralValue) {
            return dividedBy(numberValue.longValue());
        } else if (numberValue instanceof FloatingPointValue) {
            return dividedBy(numberValue.doubleValue());
        } else {
            throw new IllegalArgumentException("Cannot divide by " + numberValue);
        }
    }
}
