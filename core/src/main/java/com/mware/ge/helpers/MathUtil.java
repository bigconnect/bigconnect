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

import java.math.BigDecimal;
import java.util.Arrays;

public class MathUtil {
    private static final long NON_DOUBLE_LONG = 0xFFE0_0000_0000_0000L; // doubles are exact integers up to 53 bits

    private MathUtil() {
        throw new AssertionError();
    }

    /**
     * Calculates the portion of the first value to all values passed
     *
     * @param n The values in the set
     * @return the ratio of n[0] to the sum all n, 0 if result is {@link Double#NaN}
     */
    public static double portion(double... n) {
        assert n.length > 0;

        double first = n[0];
        if (numbersEqual(first, 0)) {
            return 0d;
        }
        double total = Arrays.stream(n).sum();
        return first / total;
    }

    public static boolean numbersEqual(double fpn, long in) {
        if (in < 0) {
            if (fpn < 0.0) {
                if ((NON_DOUBLE_LONG & in) == NON_DOUBLE_LONG) // the high order bits are only sign bits
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
                if ((NON_DOUBLE_LONG & in) == 0) // the high order bits are only sign bits
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
        if ((NON_DOUBLE_LONG & rhs) != NON_DOUBLE_LONG) {
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
}

