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
package com.mware.ge.values.utils;


import com.mware.ge.values.storable.DoubleValue;
import com.mware.ge.values.storable.IntegralValue;
import com.mware.ge.values.storable.LongValue;
import com.mware.ge.values.storable.NumberValue;

import static com.mware.ge.values.storable.Values.doubleValue;
import static com.mware.ge.values.storable.Values.longValue;

/**
 * Helper methods for doing math on Values
 */
public final class ValueMath {
    private ValueMath() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    /**
     * Overflow safe addition of two longs
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static LongValue add(long a, long b) {
        return longValue(Math.addExact(a, b));
    }

    /**
     * Overflow safe addition of two number values.
     * <p>
     * Will not overflow but instead widen the type as necessary.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static NumberValue overflowSafeAdd(NumberValue a, NumberValue b) {
        if (a instanceof IntegralValue && b instanceof IntegralValue) {
            return overflowSafeAdd(a.longValue(), b.longValue());
        } else {
            return a.plus(b);
        }
    }

    /**
     * Overflow safe addition of two longs
     * <p>
     * If the result doesn't fit in a long we widen type to use double instead.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static NumberValue overflowSafeAdd(long a, long b) {
        long r = a + b;
        //Check if result overflows
        if (((a ^ r) & (b ^ r)) < 0) {
            return doubleValue((double) a + (double) b);
        }
        return longValue(r);
    }

    /**
     * Addition of two doubles
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a + b
     */
    public static DoubleValue add(double a, double b) {
        return doubleValue(a + b);
    }

    /**
     * Overflow safe subtraction of two longs
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a - b
     */
    public static LongValue subtract(long a, long b) {
        return longValue(Math.subtractExact(a, b));
    }

    /**
     * Overflow safe subtraction of two longs
     * <p>
     * If the result doesn't fit in a long we widen type to use double instead.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a - b
     */
    public static NumberValue overflowSafeSubtract(long a, long b) {
        long r = a - b;
        //Check if result overflows
        if (((a ^ b) & (a ^ r)) < 0) {
            return doubleValue((double) a - (double) b);
        }
        return longValue(r);
    }

    /**
     * Subtraction of two doubles
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a - b
     */
    public static DoubleValue subtract(double a, double b) {
        return doubleValue(a - b);
    }

    /**
     * Overflow safe multiplication of two longs
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a * b
     */
    public static LongValue multiply(long a, long b) {
        return longValue(Math.multiplyExact(a, b));
    }

    /**
     * Overflow safe multiplication of two longs
     * <p>
     * If the result doesn't fit in a long we widen type to use double instead.
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a * b
     */
    public static NumberValue overflowSafeMultiply(long a, long b) {
        long r = a * b;
        //Check if result overflows
        long aa = Math.abs(a);
        long ab = Math.abs(b);
        if ((aa | ab) >>> 31 != 0) {
            if (((b != 0) && (r / b != a)) || (a == Long.MIN_VALUE && b == -1)) {
                return doubleValue((double) a * (double) b);
            }
        }
        return longValue(r);
    }

    /**
     * Multiplication of two doubles
     *
     * @param a left-hand operand
     * @param b right-hand operand
     * @return a * b
     */
    public static DoubleValue multiply(double a, double b) {
        return doubleValue(a * b);
    }
}
