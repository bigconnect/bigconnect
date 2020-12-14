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

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Static methods for computing the hashCode of primitive numbers and arrays of primitive numbers.
 * <p>
 * Also compares Value typed number arrays.
 */
@SuppressWarnings("WeakerAccess")
public final class NumberValues {
    private NumberValues() {
    }

    /*
     * Using the fact that the hashcode ∑x_i * 31^(i-1) can be expressed as
     * a dot product, [1, v_1, v_2, v_2, ..., v_n] • [31^n, 31^{n-1}, ..., 31, 1]. By expressing
     * it in that way the compiler is smart enough to better parallelize the
     * computation of the hash code.
     */
    static final int MAX_LENGTH = 10000;
    private static final int[] COEFFICIENTS = new int[MAX_LENGTH + 1];
    private static final long NON_DOUBLE_LONG = 0xFFE0_0000_0000_0000L; // doubles are exact integers up to 53 bits

    static {
        //We are defining the coefficient vector backwards, [1, 31, 31^2,...]
        //makes it easier and faster do find the starting position later
        COEFFICIENTS[0] = 1;
        for (int i = 1; i <= MAX_LENGTH; ++i) {
            COEFFICIENTS[i] = 31 * COEFFICIENTS[i - 1];
        }
    }

    /*
     * For equality semantics it is important that the hashcode of a long
     * is the same as the hashcode of an int as long as the long can fit in 32 bits.
     */
    public static int hash(long number) {
        int asInt = (int) number;
        if (asInt == number) {
            return asInt;
        }
        return Long.hashCode(number);
    }

    public static int hash(double number) {
        long asLong = (long) number;
        if (asLong == number) {
            return hash(asLong);
        }
        long bits = Double.doubleToLongBits(number);
        return (int) (bits ^ (bits >>> 32));
    }

    /*
     * This is a slightly silly optimization but by turning the computation
     * of the hashcode into a dot product we trick the jit compiler to use SIMD
     * instructions and performance doubles.
     */
    public static int hash(byte[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * values[i];
        }
        return result;
    }

    public static int hash(short[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * values[i];
        }
        return result;
    }

    public static int hash(char[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * values[i];
        }
        return result;
    }

    public static int hash(int[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * values[i];
        }
        return result;
    }

    public static int hash(long[] values) {
        final int max = Math.min(values.length, MAX_LENGTH);
        int result = COEFFICIENTS[max];
        for (int i = 0; i < values.length && i < COEFFICIENTS.length - 1; ++i) {
            result += COEFFICIENTS[max - i - 1] * NumberValues.hash(values[i]);
        }
        return result;
    }

    public static int hash(float[] values) {
        int result = 1;
        for (float value : values) {
            int elementHash = NumberValues.hash(value);
            result = 31 * result + elementHash;
        }
        return result;
    }

    public static int hash(double[] values) {
        int result = 1;
        for (double value : values) {
            int elementHash = NumberValues.hash(value);
            result = 31 * result + elementHash;
        }
        return result;
    }

    public static int hash(boolean[] value) {
        return Arrays.hashCode(value);
    }

    public static boolean numbersEqual(double fpn, long in) {
        if (in < 0) {
            if (fpn < 0.0) {
                if ((NON_DOUBLE_LONG & in) == 0L) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == in;
                } else if (fpn < Long.MIN_VALUE) { // the double is too big to fit in a long, they cannot be equal
                    return false;
                } else if ((fpn == Math.floor(fpn)) && !Double.isInfinite(fpn)) // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        } else {
            if (!(fpn < 0.0)) {
                if ((NON_DOUBLE_LONG & in) == 0L) // the high order bits are only sign bits
                { // no loss of precision if converting the long to a double, so it's safe to compare as double
                    return fpn == in;
                } else if (fpn > Long.MAX_VALUE) { // the double is too big to fit in a long, they cannot be equal
                    return false;
                } else if ((fpn == Math.floor(fpn)) && !Double.isInfinite(fpn))  // no decimals
                { // safe to compare as long
                    return in == (long) fpn;
                }
            }
        }
        return false;
    }

    // Tested by PropertyValueComparisonTest
    public static int compareDoubleAgainstLong(double lhs, long rhs) {
        if ((NON_DOUBLE_LONG & rhs) != 0L) {
            if (Double.isNaN(lhs)) {
                return +1;
            }
            if (Double.isInfinite(lhs)) {
                return lhs < 0 ? -1 : +1;
            }
            return BigDecimal.valueOf(lhs).compareTo(BigDecimal.valueOf(rhs));
        }
        return Double.compare(lhs, rhs);
    }

    // Tested by PropertyValueComparisonTest
    public static int compareLongAgainstDouble(long lhs, double rhs) {
        return -compareDoubleAgainstLong(rhs, lhs);
    }

    public static boolean numbersEqual(IntegralArray lhs, IntegralArray rhs) {
        int length = lhs.length();
        if (length != rhs.length()) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (lhs.longValue(i) != rhs.longValue(i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean numbersEqual(FloatingPointArray lhs, FloatingPointArray rhs) {
        int length = lhs.length();
        if (length != rhs.length()) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (lhs.doubleValue(i) != rhs.doubleValue(i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean numbersEqual(FloatingPointArray fps, IntegralArray ins) {
        int length = ins.length();
        if (length != fps.length()) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!numbersEqual(fps.doubleValue(i), ins.longValue(i))) {
                return false;
            }
        }
        return true;
    }

    public static int compareIntegerArrays(IntegralArray a, IntegralArray b) {
        int i = 0;
        int x = 0;
        int length = Math.min(a.length(), b.length());

        while (x == 0 && i < length) {
            x = Long.compare(a.longValue(i), b.longValue(i));
            i++;
        }

        if (x == 0) {
            x = a.length() - b.length();
        }

        return x;
    }

    public static int compareIntegerVsFloatArrays(IntegralArray a, FloatingPointArray b) {
        int i = 0;
        int x = 0;
        int length = Math.min(a.length(), b.length());

        while (x == 0 && i < length) {
            x = compareLongAgainstDouble(a.longValue(i), b.doubleValue(i));
            i++;
        }

        if (x == 0) {
            x = a.length() - b.length();
        }

        return x;
    }

    public static int compareFloatArrays(FloatingPointArray a, FloatingPointArray b) {
        int i = 0;
        int x = 0;
        int length = Math.min(a.length(), b.length());

        while (x == 0 && i < length) {
            x = Double.compare(a.doubleValue(i), b.doubleValue(i));
            i++;
        }

        if (x == 0) {
            x = a.length() - b.length();
        }

        return x;
    }

    public static int compareBooleanArrays(BooleanArray a, BooleanArray b) {
        int i = 0;
        int x = 0;
        int length = Math.min(a.length(), b.length());

        while (x == 0 && i < length) {
            x = Boolean.compare(a.booleanValue(i), b.booleanValue(i));
            i++;
        }

        if (x == 0) {
            x = a.length() - b.length();
        }

        return x;
    }
}
