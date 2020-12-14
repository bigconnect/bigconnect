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

import com.mware.ge.type.GeoPoint;

import java.nio.charset.StandardCharsets;
import java.time.*;

/**
 * Writer of values.
 * <p>
 * Has functionality to write all supported primitives, as well as arrays and different representations of Strings.
 *
 * @param <E> type of {@link Exception} thrown from writer methods.
 */
public interface ValueWriter<E extends Exception> {
    enum ArrayType {
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        STRING,
        CHAR,
        POINT,
        ZONED_DATE_TIME,
        LOCAL_DATE_TIME,
        DATE,
        ZONED_TIME,
        LOCAL_TIME,
        DURATION
    }

    void writeNull() throws E;

    void writeBoolean(boolean value) throws E;

    void writeInteger(byte value) throws E;

    void writeInteger(short value) throws E;

    void writeInteger(int value) throws E;

    void writeInteger(long value) throws E;

    void writeFloatingPoint(float value) throws E;

    void writeFloatingPoint(double value) throws E;

    void writeString(String value) throws E;

    void writeString(char value) throws E;

    default void writeUTF8(byte[] bytes, int offset, int length) throws E {
        writeString(new String(bytes, offset, length, StandardCharsets.UTF_8));
    }

    void beginArray(int size, ArrayType arrayType) throws E;

    void endArray() throws E;

    void writeByteArray(byte[] value) throws E;

    void writeDuration(long months, long days, long seconds, int nanos) throws E;

    void writeDate(LocalDate localDate) throws E;

    void writeLocalTime(LocalTime localTime) throws E;

    void writeTime(OffsetTime offsetTime) throws E;

    void writeLocalDateTime(LocalDateTime localDateTime) throws E;

    void writeDateTime(ZonedDateTime zonedDateTime) throws E;

    void writeGeoPoint(GeoPoint geoPoint) throws E;

    class Adapter<E extends Exception> implements ValueWriter<E> {
        @Override
        public void writeNull() throws E {   // no-op
        }

        @Override
        public void writeBoolean(boolean value) throws E {   // no-op
        }

        @Override
        public void writeInteger(byte value) throws E {   // no-op
        }

        @Override
        public void writeInteger(short value) throws E {   // no-op
        }

        @Override
        public void writeInteger(int value) throws E {   // no-op
        }

        @Override
        public void writeInteger(long value) throws E {   // no-op
        }

        @Override
        public void writeFloatingPoint(float value) throws E {   // no-op
        }

        @Override
        public void writeFloatingPoint(double value) throws E {   // no-op
        }

        @Override
        public void writeString(String value) throws E {   // no-op
        }

        @Override
        public void writeString(char value) throws E {   // no-op
        }

        @Override
        public void beginArray(int size, ArrayType arrayType) throws E {   // no-op
        }

        @Override
        public void endArray() throws E {   // no-opa
        }

        @Override
        public void writeByteArray(byte[] value) throws E {   // no-op
        }

        @Override
        public void writeDuration(long months, long days, long seconds, int nanos) {   // no-op
        }

        @Override
        public void writeDate(LocalDate localDate) throws E {   // no-op
        }

        @Override
        public void writeLocalTime(LocalTime localTime) throws E {   // no-op
        }

        @Override
        public void writeTime(OffsetTime offsetTime) throws E {   // no-op
        }

        @Override
        public void writeLocalDateTime(LocalDateTime localDateTime) throws E {   // no-op
        }

        @Override
        public void writeDateTime(ZonedDateTime zonedDateTime) throws E {   // no-op
        }

        @Override
        public void writeGeoPoint(GeoPoint geoPoint) throws E {
            // no-op
        }
    }
}
