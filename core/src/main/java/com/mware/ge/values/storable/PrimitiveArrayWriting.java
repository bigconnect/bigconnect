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

/**
 * Static methods for writing primitive arrays to a ValueWriter.
 */
public final class PrimitiveArrayWriting {
    public static <E extends Exception> void writeTo(ValueWriter<E> writer, byte[] values) throws E {
        writer.beginArray(values.length, ValueWriter.ArrayType.BYTE);
        for (byte x : values) {
            writer.writeInteger(x);
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo(ValueWriter<E> writer, short[] values) throws E {
        writer.beginArray(values.length, ValueWriter.ArrayType.SHORT);
        for (short x : values) {
            writer.writeInteger(x);
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo(ValueWriter<E> writer, int[] values) throws E {
        writer.beginArray(values.length, ValueWriter.ArrayType.INT);
        for (int x : values) {
            writer.writeInteger(x);
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo(ValueWriter<E> writer, long[] values) throws E {
        writer.beginArray(values.length, ValueWriter.ArrayType.LONG);
        for (long x : values) {
            writer.writeInteger(x);
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo(ValueWriter<E> writer, float[] values) throws E {
        writer.beginArray(values.length, ValueWriter.ArrayType.FLOAT);
        for (float x : values) {
            writer.writeFloatingPoint(x);
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo(ValueWriter<E> writer, double[] values) throws E {
        writer.beginArray(values.length, ValueWriter.ArrayType.DOUBLE);
        for (double x : values) {
            writer.writeFloatingPoint(x);
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo(ValueWriter<E> writer, boolean[] values) throws E {
        writer.beginArray(values.length, ValueWriter.ArrayType.BOOLEAN);
        for (boolean x : values) {
            writer.writeBoolean(x);
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo(ValueWriter<E> writer, char[] values) throws E {
        writer.beginArray(values.length, ValueWriter.ArrayType.CHAR);
        for (char x : values) {
            writer.writeString(x);
        }
        writer.endArray();
    }

    public static <E extends Exception> void writeTo(ValueWriter<E> writer, String[] values) throws E {
        writer.beginArray(values.length, ValueWriter.ArrayType.STRING);
        for (String x : values) {
            writer.writeString(x);
        }
        writer.endArray();
    }
}
