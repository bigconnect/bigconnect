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

import java.util.Arrays;

/**
 * Static methods for checking the equality of arrays of primitives.
 * <p>
 * This class handles only evaluation of a[] == b[] where type( a ) != type( b ), ei. byte[] == int[] and such.
 * byte[] == byte[] evaluation can be done using Arrays.equals().
 */
public final class PrimitiveArrayValues {
    private PrimitiveArrayValues() {
    }

    // TYPED COMPARISON

    public static boolean equals(byte[] a, short[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(byte[] a, int[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(byte[] a, long[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(byte[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(byte[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(short[] a, int[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(short[] a, long[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(short[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(short[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(int[] a, long[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(int[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(int[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(long[] a, float[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(long[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(float[] a, double[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(char[] a, String[] b) {
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            String str = b[i];
            if (str == null || str.length() != 1 || str.charAt(0) != a[i]) {
                return false;
            }
        }
        return true;
    }

    // NON-TYPED COMPARISON

    public static boolean equalsObject(byte[] a, Object b) {
        if (b instanceof byte[]) {
            return Arrays.equals(a, (byte[]) b);
        } else if (b instanceof short[]) {
            return equals(a, (short[]) b);
        } else if (b instanceof int[]) {
            return equals(a, (int[]) b);
        } else if (b instanceof long[]) {
            return equals(a, (long[]) b);
        } else if (b instanceof float[]) {
            return equals(a, (float[]) b);
        } else if (b instanceof double[]) {
            return equals(a, (double[]) b);
        }
        return false;
    }

    public static boolean equalsObject(short[] a, Object b) {
        if (b instanceof byte[]) {
            return equals((byte[]) b, a);
        } else if (b instanceof short[]) {
            return Arrays.equals(a, (short[]) b);
        } else if (b instanceof int[]) {
            return equals(a, (int[]) b);
        } else if (b instanceof long[]) {
            return equals(a, (long[]) b);
        } else if (b instanceof float[]) {
            return equals(a, (float[]) b);
        } else if (b instanceof double[]) {
            return equals(a, (double[]) b);
        }
        return false;
    }

    public static boolean equalsObject(int[] a, Object b) {
        if (b instanceof byte[]) {
            return equals((byte[]) b, a);
        } else if (b instanceof short[]) {
            return equals((short[]) b, a);
        } else if (b instanceof int[]) {
            return Arrays.equals(a, (int[]) b);
        } else if (b instanceof long[]) {
            return equals(a, (long[]) b);
        } else if (b instanceof float[]) {
            return equals(a, (float[]) b);
        } else if (b instanceof double[]) {
            return equals(a, (double[]) b);
        }
        return false;
    }

    public static boolean equalsObject(long[] a, Object b) {
        if (b instanceof byte[]) {
            return equals((byte[]) b, a);
        } else if (b instanceof short[]) {
            return equals((short[]) b, a);
        } else if (b instanceof int[]) {
            return equals((int[]) b, a);
        } else if (b instanceof long[]) {
            return Arrays.equals(a, (long[]) b);
        } else if (b instanceof float[]) {
            return equals(a, (float[]) b);
        } else if (b instanceof double[]) {
            return equals(a, (double[]) b);
        }
        return false;
    }

    public static boolean equalsObject(float[] a, Object b) {
        if (b instanceof byte[]) {
            return equals((byte[]) b, a);
        } else if (b instanceof short[]) {
            return equals((short[]) b, a);
        } else if (b instanceof int[]) {
            return equals((int[]) b, a);
        } else if (b instanceof long[]) {
            return equals((long[]) b, a);
        } else if (b instanceof float[]) {
            return Arrays.equals(a, (float[]) b);
        } else if (b instanceof double[]) {
            return equals(a, (double[]) b);
        }
        return false;
    }

    public static boolean equalsObject(double[] a, Object b) {
        if (b instanceof byte[]) {
            return equals((byte[]) b, a);
        } else if (b instanceof short[]) {
            return equals((short[]) b, a);
        } else if (b instanceof int[]) {
            return equals((int[]) b, a);
        } else if (b instanceof long[]) {
            return equals((long[]) b, a);
        } else if (b instanceof float[]) {
            return equals((float[]) b, a);
        } else if (b instanceof double[]) {
            return Arrays.equals(a, (double[]) b);
        }
        return false;
    }

    public static boolean equalsObject(char[] a, Object b) {
        if (b instanceof char[]) {
            return Arrays.equals(a, (char[]) b);
        } else if (b instanceof String[]) {
            return equals(a, (String[]) b);
        }
        // else if ( other instanceof String ) // should we perhaps support this?
        return false;
    }

    public static boolean equalsObject(String[] a, Object b) {
        if (b instanceof char[]) {
            return equals((char[]) b, a);
        } else if (b instanceof String[]) {
            return Arrays.equals(a, (String[]) b);
        }
        // else if ( other instanceof String ) // should we perhaps support this?
        return false;
    }
}
