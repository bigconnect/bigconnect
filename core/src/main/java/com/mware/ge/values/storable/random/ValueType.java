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

import java.util.Arrays;

import static com.mware.ge.values.storable.random.ExtremeValuesLibrary.*;

public enum ValueType
{
    BOOLEAN( ValueGroup.BOOLEAN, BooleanValue.class, EXTREME_BOOLEAN ),
    BYTE( ValueGroup.NUMBER, ByteValue.class, EXTREME_BYTE ),
    SHORT( ValueGroup.NUMBER, ShortValue.class, EXTREME_SHORT ),
    INT( ValueGroup.NUMBER, IntValue.class, EXTREME_INT ),
    LONG( ValueGroup.NUMBER, LongValue.class, EXTREME_LONG ),
    FLOAT( ValueGroup.NUMBER, FloatValue.class, EXTREME_FLOAT ),
    DOUBLE( ValueGroup.NUMBER, DoubleValue.class, EXTREME_DOUBLE ),
    CHAR( ValueGroup.TEXT, CharValue.class, EXTREME_CHAR ),
    STRING( ValueGroup.TEXT, TextValue.class, EXTREME_STRING ),
    STRING_ALPHANUMERIC( ValueGroup.TEXT, TextValue.class, EXTREME_STRING_ALPHANUMERIC ),
    STRING_ASCII( ValueGroup.TEXT, TextValue.class, EXTREME_STRING_ASCII ),
    STRING_BMP( ValueGroup.TEXT, TextValue.class, EXTREME_STRING_BMP ),
    LOCAL_DATE_TIME( ValueGroup.LOCAL_DATE_TIME, LocalDateTimeValue.class, EXTREME_LOCAL_DATE_TIME ),
    DATE( ValueGroup.DATE, DateValue.class, EXTREME_DATE ),
    LOCAL_TIME( ValueGroup.LOCAL_TIME, LocalTimeValue.class, EXTREME_LOCAL_TIME ),
    PERIOD( ValueGroup.DURATION, DurationValue.class, EXTREME_PERIOD ),
    DURATION( ValueGroup.DURATION, DurationValue.class, EXTREME_DURATION ),
    TIME( ValueGroup.ZONED_TIME, TimeValue.class, EXTREME_TIME ),
    DATE_TIME( ValueGroup.ZONED_DATE_TIME, DateTimeValue.class, EXTREME_DATE_TIME ),
    CARTESIAN_POINT( ValueGroup.GEOMETRY, GeoPointValue.class, EXTREME_CARTESIAN_POINT ),
    GEOGRAPHIC_POINT( ValueGroup.GEOMETRY, GeoPointValue.class, EXTREME_GEOGRAPHIC_POINT ),
    BOOLEAN_ARRAY( ValueGroup.BOOLEAN_ARRAY, BooleanArray.class, true, EXTREME_BOOLEAN_ARRAY ),
    BYTE_ARRAY( ValueGroup.NUMBER_ARRAY, ByteArray.class, true, EXTREME_BYTE_ARRAY ),
    SHORT_ARRAY( ValueGroup.NUMBER_ARRAY, ShortArray.class, true, EXTREME_SHORT_ARRAY ),
    INT_ARRAY( ValueGroup.NUMBER_ARRAY, IntArray.class, true, EXTREME_INT_ARRAY ),
    LONG_ARRAY( ValueGroup.NUMBER_ARRAY, LongArray.class, true, EXTREME_LONG_ARRAY ),
    FLOAT_ARRAY( ValueGroup.NUMBER_ARRAY, FloatArray.class, true, EXTREME_FLOAT_ARRAY ),
    DOUBLE_ARRAY( ValueGroup.NUMBER_ARRAY, DoubleArray.class, true, EXTREME_DOUBLE_ARRAY ),
    CHAR_ARRAY( ValueGroup.TEXT_ARRAY, CharArray.class, true, EXTREME_CHAR_ARRAY ),
    STRING_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true, EXTREME_STRING_ARRAY ),
    STRING_ALPHANUMERIC_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true, EXTREME_STRING_ALPHANUMERIC_ARRAY ),
    STRING_ASCII_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true, EXTREME_STRING_ASCII_ARRAY ),
    STRING_BMP_ARRAY( ValueGroup.TEXT_ARRAY, StringArray.class, true, EXTREME_STRING_BMP_ARRAY ),
    LOCAL_DATE_TIME_ARRAY( ValueGroup.LOCAL_DATE_TIME_ARRAY, LocalDateTimeArray.class, true, EXTREME_LOCAL_DATE_TIME_ARRAY ),
    DATE_ARRAY( ValueGroup.DATE_ARRAY, DateArray.class, true, EXTREME_DATE_ARRAY ),
    LOCAL_TIME_ARRAY( ValueGroup.LOCAL_TIME_ARRAY, LocalTimeArray.class, true, EXTREME_LOCAL_TIME_ARRAY ),
    PERIOD_ARRAY( ValueGroup.DURATION_ARRAY, DurationArray.class, true, EXTREME_PERIOD_ARRAY ),
    DURATION_ARRAY( ValueGroup.DURATION_ARRAY, DurationArray.class, true, EXTREME_DURATION_ARRAY ),
    TIME_ARRAY( ValueGroup.ZONED_TIME_ARRAY, TimeArray.class, true, EXTREME_TIME_ARRAY ),
    DATE_TIME_ARRAY( ValueGroup.ZONED_DATE_TIME_ARRAY, DateTimeArray.class, true, EXTREME_DATE_TIME_ARRAY );

    public final ValueGroup valueGroup;
    public final Class<? extends Value> valueClass;
    public final boolean arrayType;
    private final Value[] extremeValues;

    ValueType(ValueGroup valueGroup, Class<? extends Value> valueClass, Value... extremeValues )
    {
        this( valueGroup, valueClass, false, extremeValues );
    }

    ValueType(ValueGroup valueGroup, Class<? extends Value> valueClass, boolean arrayType, Value... extremeValues )
    {
        this.valueGroup = valueGroup;
        this.valueClass = valueClass;
        this.arrayType = arrayType;
        this.extremeValues = extremeValues;
    }

    public Value[] extremeValues()
    {
        return extremeValues;
    }

    static ValueType[] arrayTypes()
    {
        return Arrays.stream( ValueType.values() )
                .filter( t -> t.arrayType )
                .toArray( ValueType[]::new );
    }
}
