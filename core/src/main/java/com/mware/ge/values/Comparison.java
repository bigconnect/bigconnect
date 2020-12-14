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
package com.mware.ge.values;

/**
 * Defines the result of a ternary comparison.
 * <p>
 * In a ternary comparison the result may not only be greater than, equal or smaller than but the
 * result can also be undefined.
 */
public enum Comparison {
    GREATER_THAN {
        @Override
        public int value() {
            return 1;
        }
    },
    EQUAL {
        @Override
        public int value() {
            return 0;
        }
    },
    SMALLER_THAN {
        @Override
        public int value() {
            return -1;
        }
    },
    GREATER_THAN_AND_EQUAL,
    SMALLER_THAN_AND_EQUAL,
    UNDEFINED;

    /**
     * Integer representation of comparison
     * <p>
     * Returns a positive integer if {@link Comparison#GREATER_THAN} than, negative integer for
     * {@link Comparison#SMALLER_THAN},
     * and zero for {@link Comparison#EQUAL}
     *
     * @return a positive number if result is greater than, a negative number if the result is smaller than or zero
     * if equal.
     * @throws IllegalStateException if the result is undefined.
     */
    public int value() {
        throw new IllegalStateException("This value is undefined and can't handle primitive comparisons");
    }

    /**
     * Maps an integer value to comparison result.
     *
     * @param i the integer to be mapped to a Comparison
     * @return {@link Comparison#GREATER_THAN} than if positive, {@link Comparison#SMALLER_THAN} if negative or
     * {@link Comparison#EQUAL} if zero
     */
    public static Comparison from(int i) {
        if (i > 0) {
            return GREATER_THAN;
        } else if (i < 0) {
            return SMALLER_THAN;
        } else {
            return EQUAL;
        }
    }
}
