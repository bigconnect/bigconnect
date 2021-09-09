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

import com.mware.ge.Range;
import com.mware.ge.type.*;

import java.lang.reflect.Array;
import java.time.*;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Objects;

import static com.mware.ge.values.storable.DateTimeValue.datetime;
import static com.mware.ge.values.storable.DateValue.date;
import static com.mware.ge.values.storable.DurationValue.duration;
import static com.mware.ge.values.storable.LocalDateTimeValue.localDateTime;
import static com.mware.ge.values.storable.LocalTimeValue.localTime;
import static com.mware.ge.values.storable.TimeValue.time;
import static java.lang.String.format;

/**
 * Entry point to the values library.
 * <p>
 * The values library centers around the Value class, which represents a value in Neo4j. Values can be correctly
 * checked for equality over different primitive representations, including consistent hashCodes and sorting.
 * <p>
 * To create Values use the factory methods in the Values class.
 * <p>
 * Values come in two major categories: Storable and Virtual. Storable values are valid values for
 * node, relationship and graph properties. Virtual values are not supported as property values, but might be created
 * and returned as part of cypher execution. These include Node, Relationship and Path.
 */
@SuppressWarnings("WeakerAccess")
public final class Values {
    public static final Value MIN_NUMBER = Values.doubleValue(Double.NEGATIVE_INFINITY);
    public static final Value MAX_NUMBER = Values.doubleValue(Double.NaN);
    public static final Value ZERO_FLOAT = Values.doubleValue(0.0);
    public static final IntegralValue ZERO_INT = Values.longValue(0);
    public static final Value MIN_STRING = StringValue.EMPTY;
    public static final Value MAX_STRING = Values.booleanValue(false);
    public static final BooleanValue TRUE = Values.booleanValue(true);
    public static final BooleanValue FALSE = Values.booleanValue(false);
    public static final TextValue EMPTY_STRING = StringValue.EMPTY;
    public static final DoubleValue E = Values.doubleValue(Math.E);
    public static final DoubleValue PI = Values.doubleValue(Math.PI);
    public static final ArrayValue EMPTY_SHORT_ARRAY = Values.shortArray(new short[0]);
    public static final ArrayValue EMPTY_BOOLEAN_ARRAY = Values.booleanArray(new boolean[0]);
    public static final ArrayValue EMPTY_BYTE_ARRAY = Values.byteArray(new byte[0]);
    public static final ArrayValue EMPTY_CHAR_ARRAY = Values.charArray(new char[0]);
    public static final ArrayValue EMPTY_INT_ARRAY = Values.intArray(new int[0]);
    public static final ArrayValue EMPTY_LONG_ARRAY = Values.longArray(new long[0]);
    public static final ArrayValue EMPTY_FLOAT_ARRAY = Values.floatArray(new float[0]);
    public static final ArrayValue EMPTY_DOUBLE_ARRAY = Values.doubleArray(new double[0]);
    public static final TextArray EMPTY_TEXT_ARRAY = Values.stringArray();

    private Values() {
    }

    /**
     * Default value comparator. Will correctly compare all storable values and order the value groups according the
     * to orderability group.
     * <p>
     * To get Comparability semantics, use .ternaryCompare
     */
    public static final ValueComparator COMPARATOR = new ValueComparator(ValueGroup::compareTo);

    public static boolean isNumberValue(Object value) {
        return value instanceof NumberValue;
    }

    public static boolean isBooleanValue(Object value) {
        return value instanceof BooleanValue;
    }

    public static boolean isTextValue(Object value) {
        return value instanceof TextValue;
    }

    public static boolean isArrayValue(Value value) {
        return value instanceof ArrayValue;
    }

    public static boolean isTemporalValue(Value value) {
        return value instanceof TemporalValue || value instanceof DurationValue;
    }

    public static boolean isTemporalArray(Value value) {
        return value instanceof TemporalArray || value instanceof DurationArray;
    }

    public static double coerceToDouble(Value value) {
        if (value instanceof IntegralValue) {
            return ((IntegralValue) value).longValue();
        }
        if (value instanceof FloatingPointValue) {
            return ((FloatingPointValue) value).doubleValue();
        }
        throw new UnsupportedOperationException(format("Cannot coerce %s to double", value));
    }

    // DIRECT FACTORY METHODS

    public static final Value NO_VALUE = NoValue.NO_VALUE;

    public static TextValue utf8Value(byte[] bytes) {
        if (bytes.length == 0) {
            return EMPTY_STRING;
        }

        return utf8Value(bytes, 0, bytes.length);
    }

    public static TextValue utf8Value(byte[] bytes, int offset, int length) {
        if (length == 0) {
            return EMPTY_STRING;
        }

        return new UTF8StringValue(bytes, offset, length);
    }

    public static GeoPointValue geoPointValue(double lat, double lon) {
        return new GeoPointValue(new GeoPoint(lat, lon));
    }

    public static GeoPointValue geoPointValue(GeoPoint geoPoint) {
        return new GeoPointValue(geoPoint);
    }

    public static GeoCircleValue geoCircleValue(double latitude, double longitude, double radius) {
        return new GeoCircleValue(new GeoCircle(latitude, longitude, radius));
    }

    public static GeoCircleValue geoCircleValue(GeoCircle geoCircle) {
        return new GeoCircleValue(geoCircle);
    }

    public static GeoRectValue geoRectValue(GeoPoint northWest, GeoPoint southEast) {
        return new GeoRectValue(new GeoRect(northWest, southEast));
    }

    public static GeoRectValue geoRectValue(GeoRect geoRect) {
        return new GeoRectValue(geoRect);
    }

    public static GeoHashValue geoHashValue(GeoHash geoHash) {
        return new GeoHashValue(geoHash);
    }

    public static GeoCollectionValue geoCollectionValue(GeoCollection geoCollection) {
        return new GeoCollectionValue(geoCollection);
    }

    public static GeoPolygonValue geoPolygonValue(GeoPolygon geoPolygon) {
        return new GeoPolygonValue(geoPolygon);
    }

    public static GeoLineValue geoLineValue(GeoLine geoLine) {
        return new GeoLineValue(geoLine);
    }

    public static GeoShapeValue geoShapeValue(GeoShape geoShape) {
        if (geoShape instanceof GeoPoint)
            return geoPointValue((GeoPoint) geoShape);
        else if (geoShape instanceof GeoCircle)
            return geoCircleValue((GeoCircle) geoShape);
        else if (geoShape instanceof GeoRect)
            return geoRectValue((GeoRect) geoShape);
        else if (geoShape instanceof GeoPolygon)
            return geoPolygonValue((GeoPolygon) geoShape);
        else if (geoShape instanceof GeoCollection)
            return geoCollectionValue((GeoCollection) geoShape);
        else if (geoShape instanceof GeoHash)
            return geoHashValue((GeoHash) geoShape);
        else if (geoShape instanceof GeoLine)
            return geoLineValue((GeoLine) geoShape);
        else
            throw new IllegalArgumentException("Don't know how to handle geoshape: "+geoShape.getClass());
    }

    public static TextValue stringValue(String value) {
        if (value == null || value.isEmpty()) {
            return EMPTY_STRING;
        }
        return new StringWrappingStringValue(value);
    }

    public static Value stringOrNoValue(String value) {
        if (value == null) {
            return NO_VALUE;
        } else {
            return stringValue(value);
        }
    }

    public static NumberValue numberValue(Number number) {
        if (number instanceof Long) {
            return longValue(number.longValue());
        }
        if (number instanceof Integer) {
            return intValue(number.intValue());
        }
        if (number instanceof Double) {
            return doubleValue(number.doubleValue());
        }
        if (number instanceof Byte) {
            return byteValue(number.byteValue());
        }
        if (number instanceof Float) {
            return floatValue(number.floatValue());
        }
        if (number instanceof Short) {
            return shortValue(number.shortValue());
        }

        throw new UnsupportedOperationException("Unsupported type of Number " + number.toString());
    }

    public static LongValue longValue(long value) {
        return new LongValue(value);
    }

    public static IntValue intValue(int value) {
        return new IntValue(value);
    }

    public static ShortValue shortValue(short value) {
        return new ShortValue(value);
    }

    public static ByteValue byteValue(byte value) {
        return new ByteValue(value);
    }

    public static BooleanValue booleanValue(boolean value) {
        return value ? BooleanValue.TRUE : BooleanValue.FALSE;
    }

    public static CharValue charValue(char value) {
        return new CharValue(value);
    }

    public static DoubleValue doubleValue(double value) {
        return new DoubleValue(value);
    }

    public static FloatValue floatValue(float value) {
        return new FloatValue(value);
    }

    public static TextArray stringArray(String... value) {
        return new StringArray(value);
    }

    public static ByteArray byteArray(byte[] value) {
        return new ByteArray(value);
    }

    public static LongArray longArray(long[] value) {
        return new LongArray(value);
    }

    public static IntArray intArray(int[] value) {
        return new IntArray(value);
    }

    public static DoubleArray doubleArray(double[] value) {
        return new DoubleArray(value);
    }

    public static FloatArray floatArray(float[] value) {
        return new FloatArray(value);
    }

    public static BooleanArray booleanArray(boolean[] value) {
        return new BooleanArray(value);
    }

    public static CharArray charArray(char[] value) {
        return new CharArray(value);
    }

    public static ShortArray shortArray(short[] value) {
        return new ShortArray(value);
    }

    public static Value temporalRangeValue(Range range) {
        if (range.getStart() instanceof ZonedDateTime && range.getEnd() instanceof ZonedDateTime) {
            return dateTimeArray(new ZonedDateTime[] {(ZonedDateTime)range.getStart(), (ZonedDateTime)range.getEnd()});
        } else if (range.getStart() instanceof LocalDateTime && range.getEnd() instanceof LocalDateTime) {
            return localDateTimeArray(new LocalDateTime[] { (LocalDateTime)range.getStart(), (LocalDateTime)range.getEnd() });
        }  else if (range.getStart() instanceof LocalDate && range.getEnd() instanceof LocalDate) {
            return dateArray(new LocalDate[] { (LocalDate)range.getStart(), (LocalDate)range.getEnd() });
        }
        else {
            throw new IllegalArgumentException("Range must contain Temporal values");
        }
    }

    public static Value temporalValue(Temporal value) {
        if (value instanceof ZonedDateTime) {
            return datetime((ZonedDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return datetime((OffsetDateTime) value);
        }
        if (value instanceof LocalDateTime) {
            return localDateTime((LocalDateTime) value);
        }
        if (value instanceof OffsetTime) {
            return time((OffsetTime) value);
        }
        if (value instanceof LocalDate) {
            return date((LocalDate) value);
        }
        if (value instanceof LocalTime) {
            return localTime((LocalTime) value);
        }
        if (value instanceof TemporalValue) {
            return (Value) value;
        }
        if (value == null) {
            return NO_VALUE;
        }

        throw new UnsupportedOperationException("Unsupported type of Temporal " + value.toString());
    }

    public static DurationValue durationValue(TemporalAmount value) {
        if (value instanceof Duration) {
            return duration((Duration) value);
        }
        if (value instanceof Period) {
            return duration((Period) value);
        }
        if (value instanceof DurationValue) {
            return (DurationValue) value;
        }
        DurationValue duration = duration(0, 0, 0, 0);
        for (TemporalUnit unit : value.getUnits()) {
            duration = duration.plus(value.get(unit), unit);
        }
        return duration;
    }

    public static DateTimeArray dateTimeArray(ZonedDateTime[] values) {
        return new DateTimeArray(values);
    }

    public static LocalDateTimeArray localDateTimeArray(LocalDateTime[] values) {
        return new LocalDateTimeArray(values);
    }

    public static LocalTimeArray localTimeArray(LocalTime[] values) {
        return new LocalTimeArray(values);
    }

    public static TimeArray timeArray(OffsetTime[] values) {
        return new TimeArray(values);
    }

    public static DateArray dateArray(LocalDate[] values) {
        return new DateArray(values);
    }

    public static DurationArray durationArray(DurationValue[] values) {
        return new DurationArray(values);
    }

    public static DurationArray durationArray(TemporalAmount[] values) {
        DurationValue[] durations = new DurationValue[values.length];
        for (int i = 0; i < values.length; i++) {
            durations[i] = durationValue(values[i]);
        }
        return new DurationArray(durations);
    }

    // BOXED FACTORY METHODS

    /**
     * Generic value factory method.
     * <p>
     * Beware, this method is intended for converting externally supplied values to the internal Value type, and to
     * make testing convenient. Passing a Value as in parameter should never be needed, and will throw an
     * UnsupportedOperationException.
     * <p>
     * This method does defensive copying of arrays, while the explicit *Array() factory methods do not.
     *
     * @param value Object to convert to Value
     * @return the created Value
     */
    public static Value of(Object value) {
        return of(value, true);
    }

    public static Value of(Object value, boolean allowNull) {
        Value of = unsafeOf(value, allowNull);
        if (of != null) {
            return of;
        }
        Objects.requireNonNull(value);
        throw new IllegalArgumentException(
                format("[%s:%s] is not a supported property value", value, value.getClass().getName()));
    }

    public static Value unsafeOf(Object value, boolean allowNull) {
        if (value instanceof String) {
            return stringValue((String) value);
        }
        if (value instanceof Object[]) {
            return arrayValue((Object[]) value);
        }
        if (value instanceof Boolean) {
            return booleanValue((Boolean) value);
        }
        if (value instanceof Number) {
            return numberValue((Number) value);
        }
        if (value instanceof Character) {
            return charValue((Character) value);
        }
        if (value instanceof Temporal) {
            return temporalValue((Temporal) value);
        }
        if (value instanceof Range) {
            return temporalRangeValue((Range) value);
        }
        if (value instanceof TemporalAmount) {
            return durationValue((TemporalAmount) value);
        }
        if (value instanceof GeoPoint) {
            return geoPointValue((GeoPoint) value);
        }
        if (value instanceof GeoCircle) {
            return geoCircleValue((GeoCircle) value);
        }
        if (value instanceof GeoLine) {
            return geoLineValue((GeoLine) value);
        }
        if (value instanceof GeoRect) {
            return geoRectValue((GeoRect) value);
        }
        if (value instanceof GeoPolygon) {
            return geoPolygonValue((GeoPolygon) value);
        }
        if (value instanceof GeoCollection) {
            return geoCollectionValue((GeoCollection) value);
        }
        if (value instanceof GeoHash) {
            return geoHashValue((GeoHash) value);
        }
        if (value instanceof byte[]) {
            return byteArray(((byte[]) value).clone());
        }
        if (value instanceof long[]) {
            return longArray(((long[]) value).clone());
        }
        if (value instanceof int[]) {
            return intArray(((int[]) value).clone());
        }
        if (value instanceof double[]) {
            return doubleArray(((double[]) value).clone());
        }
        if (value instanceof float[]) {
            return floatArray(((float[]) value).clone());
        }
        if (value instanceof boolean[]) {
            return booleanArray(((boolean[]) value).clone());
        }
        if (value instanceof char[]) {
            return charArray(((char[]) value).clone());
        }
        if (value instanceof short[]) {
            return shortArray(((short[]) value).clone());
        }
        if (value == null) {
            if (allowNull) {
                return NoValue.NO_VALUE;
            }
            throw new IllegalArgumentException("[null] is not a supported property value");
        }
        if (value instanceof Value) {
            throw new UnsupportedOperationException(
                    "Converting a Value to a Value using Values.of() is not supported.");
        }

        // otherwise fail
        return null;
    }

    /**
     * Generic value factory method.
     * <p>
     * Converts an array of object values to the internal Value type. See {@link Values#of}.
     */
    public static Value[] values(Object... objects) {
        return Arrays.stream(objects)
                .map(Values::of)
                .toArray(Value[]::new);
    }

    @Deprecated
        public static Object asObject(Value value) {
        return value == null ? null : value.asObject();
    }

    public static Object[] asObjects(Value[] propertyValues) {
        Object[] legacy = new Object[propertyValues.length];

        for (int i = 0; i < propertyValues.length; i++) {
            legacy[i] = propertyValues[i].asObjectCopy();
        }

        return legacy;
    }

    private static Value arrayValue(Object[] value) {
        if (value instanceof String[]) {
            return stringArray(copy(value, new String[value.length]));
        }
        if (value instanceof Byte[]) {
            return byteArray(copy(value, new byte[value.length]));
        }
        if (value instanceof Long[]) {
            return longArray(copy(value, new long[value.length]));
        }
        if (value instanceof Integer[]) {
            return intArray(copy(value, new int[value.length]));
        }
        if (value instanceof Double[]) {
            return doubleArray(copy(value, new double[value.length]));
        }
        if (value instanceof Float[]) {
            return floatArray(copy(value, new float[value.length]));
        }
        if (value instanceof Boolean[]) {
            return booleanArray(copy(value, new boolean[value.length]));
        }
        if (value instanceof Character[]) {
            return charArray(copy(value, new char[value.length]));
        }
        if (value instanceof Short[]) {
            return shortArray(copy(value, new short[value.length]));
        }
        if (value instanceof ZonedDateTime[]) {
            return dateTimeArray(copy(value, new ZonedDateTime[value.length]));
        }
        if (value instanceof LocalDateTime[]) {
            return localDateTimeArray(copy(value, new LocalDateTime[value.length]));
        }
        if (value instanceof LocalTime[]) {
            return localTimeArray(copy(value, new LocalTime[value.length]));
        }
        if (value instanceof OffsetTime[]) {
            return timeArray(copy(value, new OffsetTime[value.length]));
        }
        if (value instanceof LocalDate[]) {
            return dateArray(copy(value, new LocalDate[value.length]));
        }
        if (value instanceof TemporalAmount[]) {
            // no need to copy here, since the durationArray(...) method will perform copying as appropriate
            return durationArray((TemporalAmount[]) value);
        }
        if (value.length > 0) {
            // try to guess the type of array based on the first element
            Class<?> etype = value[0].getClass();
            Object o = Array.newInstance(etype, value.length);
            copy(value, o);
            return arrayValue((Object[]) o);
        }
        return null;
    }

    private static <T> T copy(Object[] value, T target) {
        for (int i = 0; i < value.length; i++) {
            if (value[i] == null) {
                throw new IllegalArgumentException("Property array value elements may not be null.");
            }
            Array.set(target, i, value[i]);
        }
        return target;
    }

    public static Value minValue(ValueGroup valueGroup, Value value) {
        switch (valueGroup) {
            case TEXT:
                return MIN_STRING;
            case NUMBER:
                return MIN_NUMBER;
            case DATE:
                return DateValue.MIN_VALUE;
            case LOCAL_DATE_TIME:
                return LocalDateTimeValue.MIN_VALUE;
            case ZONED_DATE_TIME:
                return DateTimeValue.MIN_VALUE;
            case LOCAL_TIME:
                return LocalTimeValue.MIN_VALUE;
            case ZONED_TIME:
                return TimeValue.MIN_VALUE;
            default:
                throw new IllegalStateException(
                        format("The minValue for valueGroup %s is not defined yet", valueGroup));
        }
    }

    public static Value maxValue(ValueGroup valueGroup, Value value) {
        switch (valueGroup) {
            case TEXT:
                return MAX_STRING;
            case NUMBER:
                return MAX_NUMBER;
            case DATE:
                return DateValue.MAX_VALUE;
            case LOCAL_DATE_TIME:
                return LocalDateTimeValue.MAX_VALUE;
            case ZONED_DATE_TIME:
                return DateTimeValue.MAX_VALUE;
            case LOCAL_TIME:
                return LocalTimeValue.MAX_VALUE;
            case ZONED_TIME:
                return TimeValue.MAX_VALUE;
            default:
                throw new IllegalStateException(
                        format("The maxValue for valueGroup %s is not defined yet", valueGroup));
        }
    }
}
