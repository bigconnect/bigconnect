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

import com.mware.ge.util.Preconditions;

public class Numbers {

    /**
     * Checks if {@code value} is a power of 2.
     *
     * @param value the value to check
     * @return {@code true} if {@code value} is a power of 2.
     */
    public static boolean isPowerOfTwo(long value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    /**
     * Returns base 2 logarithm of the closest power of 2 that is less or equal to the {@code value}.
     *
     * @param value a positive long value
     */
    public static int log2floor(long value) {
        return (Long.SIZE - 1) - Long.numberOfLeadingZeros(Preconditions.requirePositive(value));
    }

    public static short safeCastIntToUnsignedShort(int value) {
        if ((value & ~0xFFFF) != 0) {
            throw new ArithmeticException(getOverflowMessage(value, "unsigned short"));
        }
        return (short) value;
    }

    public static byte safeCastIntToUnsignedByte(int value) {
        if ((value & ~0xFF) != 0) {
            throw new ArithmeticException(getOverflowMessage(value, "unsigned byte"));
        }
        return (byte) value;
    }

    public static int safeCastLongToInt(long value) {
        if ((int) value != value) {
            throw new ArithmeticException(getOverflowMessage(value, Integer.TYPE));
        }
        return (int) value;
    }

    public static short safeCastLongToShort(long value) {
        if ((short) value != value) {
            throw new ArithmeticException(getOverflowMessage(value, Short.TYPE));
        }
        return (short) value;
    }

    public static short safeCastIntToShort(int value) {
        if ((short) value != value) {
            throw new ArithmeticException(getOverflowMessage(value, Short.TYPE));
        }
        return (short) value;
    }

    public static byte safeCastLongToByte(long value) {
        if ((byte) value != value) {
            throw new ArithmeticException(getOverflowMessage(value, Byte.TYPE));
        }
        return (byte) value;
    }

    public static int unsignedShortToInt(short value) {
        return value & 0xFFFF;
    }

    public static int unsignedByteToInt(byte value) {
        return value & 0xFF;
    }

    private static String getOverflowMessage(long value, Class<?> clazz) {
        return getOverflowMessage(value, clazz.getName());
    }

    private static String getOverflowMessage(long value, String numericType) {
        return "Value " + value + " is too big to be represented as " + numericType;
    }
}
