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
package com.mware.ge.values.storable.random;

import com.mware.ge.values.storable.*;

import java.time.*;

class ExtremeValuesLibrary {
    private ExtremeValuesLibrary() {
    }

    private static final int MAX_ALPHA_NUMERIC_CODE_POINT = 0x7a; // small z
    private static final String MAX_CODE_POINT_STRING = new String(new int[]{Character.MAX_CODE_POINT}, 0, 1);
    private static final String MAX_ALPHA_NUMERIC_CODE_POINT_STRING = new String(new int[]{MAX_ALPHA_NUMERIC_CODE_POINT}, 0, 1);
    private static final String MAX_ASCII_CODE_POINT_STRING = new String(new int[]{RandomValues.MAX_ASCII_CODE_POINT}, 0, 1);
    private static final String MAX_BMP_CODE_POINT_STRING = new String(new int[]{RandomValues.MAX_BMP_CODE_POINT}, 0, 1);

    static final Value[] EXTREME_BOOLEAN = new Value[]{
            Values.booleanValue(false),
            Values.booleanValue(true)
    };
    static final Value[] EXTREME_BYTE = new Value[]{
            Values.byteValue(Byte.MIN_VALUE),
            Values.byteValue(Byte.MAX_VALUE),
            Values.byteValue((byte) 0)
    };
    static final Value[] EXTREME_SHORT = new Value[]{
            Values.shortValue(Short.MIN_VALUE),
            Values.shortValue(Short.MAX_VALUE),
            Values.shortValue((short) 0)
    };
    static final Value[] EXTREME_INT = new Value[]{
            Values.intValue(Integer.MIN_VALUE),
            Values.intValue(Integer.MAX_VALUE),
            Values.intValue(0)
    };
    static final Value[] EXTREME_LONG = new Value[]{
            Values.longValue(Long.MIN_VALUE),
            Values.longValue(Long.MAX_VALUE),
            Values.longValue(0)
    };
    static final Value[] EXTREME_FLOAT = new Value[]{
            Values.floatValue(Float.MIN_VALUE),
            Values.floatValue(Float.MAX_VALUE),
            Values.floatValue(-Float.MIN_VALUE),
            Values.floatValue(-Float.MAX_VALUE),
            Values.floatValue(0f)
    };
    static final Value[] EXTREME_DOUBLE = new Value[]{
            Values.doubleValue(Double.MIN_VALUE),
            Values.doubleValue(Double.MAX_VALUE),
            Values.doubleValue(-Double.MIN_VALUE),
            Values.doubleValue(-Double.MAX_VALUE),
            Values.doubleValue(0d)
    };
    static final Value[] EXTREME_CHAR = new Value[]{
            Values.charValue(Character.MAX_VALUE),
            Values.charValue(Character.MIN_VALUE)
    };
    static final Value[] EXTREME_STRING = new Value[]{
            Values.stringValue(MAX_CODE_POINT_STRING),
            Values.stringValue("")
    };
    static final Value[] EXTREME_STRING_ALPHANUMERIC = new Value[]{
            Values.stringValue(MAX_ALPHA_NUMERIC_CODE_POINT_STRING),
            Values.stringValue("")
    };
    static final Value[] EXTREME_STRING_ASCII = new Value[]{
            Values.stringValue(MAX_ASCII_CODE_POINT_STRING),
            Values.stringValue("")
    };
    static final Value[] EXTREME_STRING_BMP = new Value[]{
            Values.stringValue(MAX_BMP_CODE_POINT_STRING),
            Values.stringValue("")
    };
    static final Value[] EXTREME_LOCAL_DATE_TIME = new Value[]{
            LocalDateTimeValue.MIN_VALUE,
            LocalDateTimeValue.MAX_VALUE
    };
    static final Value[] EXTREME_DATE = new Value[]{
            DateValue.MIN_VALUE,
            DateValue.MAX_VALUE
    };
    static final Value[] EXTREME_LOCAL_TIME = new Value[]{
            LocalTimeValue.MIN_VALUE,
            LocalTimeValue.MAX_VALUE
    };
    static final Value[] EXTREME_PERIOD = new Value[]{
            DurationValue.MIN_VALUE,
            DurationValue.MAX_VALUE
    };
    static final Value[] EXTREME_DURATION = new Value[]{
            DurationValue.MIN_VALUE,
            DurationValue.MAX_VALUE
    };
    static final Value[] EXTREME_TIME = new Value[]{
            TimeValue.MIN_VALUE,
            TimeValue.MAX_VALUE
    };
    static final Value[] EXTREME_DATE_TIME = new Value[]{
            DateTimeValue.MIN_VALUE,
            DateTimeValue.MAX_VALUE
    };
    static final Value[] EXTREME_CARTESIAN_POINT = new Value[]{
            GeoPointValue.MIN_VALUE_CARTESIAN,
            GeoPointValue.MAX_VALUE_CARTESIAN
    };
    static final Value[] EXTREME_GEOGRAPHIC_POINT = new Value[]{
            GeoPointValue.MIN_VALUE_WGS84,
            GeoPointValue.MAX_VALUE_WGS84
    };
    static final Value[] EXTREME_BOOLEAN_ARRAY = new Value[]{
            Values.of(new boolean[0]),
            Values.of(new boolean[]{true})
    };
    static final Value[] EXTREME_BYTE_ARRAY = new Value[]{
            Values.of(new byte[0]),
            Values.of(new byte[]{Byte.MAX_VALUE})
    };
    static final Value[] EXTREME_SHORT_ARRAY = new Value[]{
            Values.of(new short[0]),
            Values.of(new short[]{Short.MAX_VALUE})
    };
    static final Value[] EXTREME_INT_ARRAY = new Value[]{
            Values.of(new int[0]),
            Values.of(new int[]{Integer.MAX_VALUE})
    };
    static final Value[] EXTREME_LONG_ARRAY = new Value[]{
            Values.of(new long[0]),
            Values.of(new long[]{Long.MAX_VALUE})
    };
    static final Value[] EXTREME_FLOAT_ARRAY = new Value[]{
            Values.of(new float[0]),
            Values.of(new float[]{Float.MAX_VALUE})
    };
    static final Value[] EXTREME_DOUBLE_ARRAY = new Value[]{
            Values.of(new double[0]),
            Values.of(new double[]{Double.MAX_VALUE})
    };
    static final Value[] EXTREME_CHAR_ARRAY = new Value[]{
            Values.of(new char[0]),
            Values.of(new char[]{Character.MAX_VALUE})
    };
    static final Value[] EXTREME_STRING_ARRAY = new Value[]{
            Values.of(new String[0]),
            Values.of(new String[]{MAX_CODE_POINT_STRING})
    };
    static final Value[] EXTREME_STRING_ALPHANUMERIC_ARRAY = new Value[]{
            Values.of(new String[0]),
            Values.of(new String[]{MAX_ALPHA_NUMERIC_CODE_POINT_STRING})
    };
    static final Value[] EXTREME_STRING_ASCII_ARRAY = new Value[]{
            Values.of(new String[0]),
            Values.of(new String[]{MAX_ASCII_CODE_POINT_STRING})
    };
    static final Value[] EXTREME_STRING_BMP_ARRAY = new Value[]{
            Values.of(new String[0]),
            Values.of(new String[]{MAX_BMP_CODE_POINT_STRING})
    };
    static final Value[] EXTREME_LOCAL_DATE_TIME_ARRAY = new Value[]{
            Values.of(new LocalDateTime[0]),
            Values.of(new LocalDateTime[]{LocalDateTime.MAX})
    };
    static final Value[] EXTREME_DATE_ARRAY = new Value[]{
            Values.of(new LocalDate[0]),
            Values.of(new LocalDate[]{LocalDate.MAX})
    };
    static final Value[] EXTREME_LOCAL_TIME_ARRAY = new Value[]{
            Values.of(new LocalTime[0]),
            Values.of(new LocalTime[]{LocalTime.MAX})
    };
    static final Value[] EXTREME_PERIOD_ARRAY = new Value[]{
            Values.of(new DurationValue[0]),
            Values.of(new DurationValue[]{DurationValue.MAX_VALUE})
    };
    static final Value[] EXTREME_DURATION_ARRAY = new Value[]{
            Values.of(new DurationValue[0]),
            Values.of(new DurationValue[]{DurationValue.MAX_VALUE})
    };
    static final Value[] EXTREME_TIME_ARRAY = new Value[]{
            Values.of(new OffsetTime[0]),
            Values.of(new OffsetTime[]{OffsetTime.MAX})
    };
    static final Value[] EXTREME_DATE_TIME_ARRAY = new Value[]{
            Values.of(new ZonedDateTime[0]),
            Values.of(new ZonedDateTime[]{ZonedDateTime.of(LocalDateTime.MAX, ZoneOffset.MAX)})
    };
}
