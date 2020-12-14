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

import com.mware.ge.values.storable.*;
import com.mware.ge.values.virtual.MapValue;
import com.mware.ge.values.virtual.PathValue;
import com.mware.ge.values.virtual.VirtualNodeValue;
import com.mware.ge.values.virtual.VirtualRelationshipValue;

import java.time.*;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mware.ge.values.storable.Values.stringValue;


public interface ValueMapper<Base> {
    // Virtual

    Base mapPath(PathValue value);

    Base mapNode(VirtualNodeValue value);

    Base mapRelationship(VirtualRelationshipValue value);

    Base mapMap(MapValue value);

    // Storable

    Base mapNoValue();

    Base mapSequence(SequenceValue value);

    Base mapText(TextValue value);

    default Base mapString(StringValue value) {
        return mapText(value);
    }

    default Base mapTextArray(TextArray value) {
        return mapSequence(value);
    }

    default Base mapStringArray(StringArray value) {
        return mapTextArray(value);
    }

    default Base mapChar(CharValue value) {
        return mapText(value);
    }

    default Base mapCharArray(CharArray value) {
        return mapTextArray(value);
    }

    Base mapBoolean(BooleanValue value);

    default Base mapBooleanArray(BooleanArray value) {
        return mapSequence(value);
    }

    Base mapNumber(NumberValue value);

    default Base mapNumberArray(NumberArray value) {
        return mapSequence(value);
    }

    default Base mapIntegral(IntegralValue value) {
        return mapNumber(value);
    }

    default Base mapIntegralArray(IntegralArray value) {
        return mapNumberArray(value);
    }

    default Base mapByte(ByteValue value) {
        return mapIntegral(value);
    }

    default Base mapByteArray(ByteArray value) {
        return mapIntegralArray(value);
    }

    default Base mapShort(ShortValue value) {
        return mapIntegral(value);
    }

    default Base mapShortArray(ShortArray value) {
        return mapIntegralArray(value);
    }

    default Base mapInt(IntValue value) {
        return mapIntegral(value);
    }

    default Base mapIntArray(IntArray value) {
        return mapIntegralArray(value);
    }

    default Base mapLong(LongValue value) {
        return mapIntegral(value);
    }

    default Base mapLongArray(LongArray value) {
        return mapIntegralArray(value);
    }

    default Base mapFloatingPoint(FloatingPointValue value) {
        return mapNumber(value);
    }

    default Base mapFloatingPointArray(FloatingPointArray value) {
        return mapNumberArray(value);
    }

    default Base mapDouble(DoubleValue value) {
        return mapFloatingPoint(value);
    }

    default Base mapDoubleArray(DoubleArray value) {
        return mapFloatingPointArray(value);
    }

    default Base mapFloat(FloatValue value) {
        return mapFloatingPoint(value);
    }

    default Base mapFloatArray(FloatArray value) {
        return mapFloatingPointArray(value);
    }

    Base mapDateTime(DateTimeValue value);

    Base mapLocalDateTime(LocalDateTimeValue value);

    Base mapDate(DateValue value);

    Base mapTime(TimeValue value);

    Base mapLocalTime(LocalTimeValue value);

    Base mapDuration(DurationValue value);

    default Base mapDateTimeArray(DateTimeArray value) {
        return mapSequence(value);
    }

    default Base mapLocalDateTimeArray(LocalDateTimeArray value) {
        return mapSequence(value);
    }

    default Base mapLocalTimeArray(LocalTimeArray value) {
        return mapSequence(value);
    }

    default Base mapTimeArray(TimeArray value) {
        return mapSequence(value);
    }

    default Base mapDateArray(DateArray value) {
        return mapSequence(value);
    }

    default Base mapDurationArray(DurationArray value) {
        return mapSequence(value);
    }

    default Base mapGeoPoint(GeoPointValue geoPointValue) {
        return mapText(stringValue(geoPointValue.prettyPrint()));
    }

    ;

    abstract class JavaMapper implements ValueMapper<Object> {
        @Override
        public Object mapNoValue() {
            return null;
        }

        @Override
        public Object mapMap(MapValue value) {
            Map<Object, Object> map = new HashMap<>();
            value.foreach((k, v) -> map.put(k, v.map(this)));
            return map;
        }

        @Override
        public List<?> mapSequence(SequenceValue value) {
            List<Object> list = new ArrayList<>(value.length());
            value.forEach(v -> list.add(v.map(this)));
            return list;
        }

        @Override
        public Character mapChar(CharValue value) {
            return value.value();
        }

        @Override
        public String mapText(TextValue value) {
            return value.stringValue();
        }

        @Override
        public String[] mapStringArray(StringArray value) {
            return value.asObjectCopy();
        }

        @Override
        public char[] mapCharArray(CharArray value) {
            return value.asObjectCopy();
        }

        @Override
        public Boolean mapBoolean(BooleanValue value) {
            return value.booleanValue();
        }

        @Override
        public boolean[] mapBooleanArray(BooleanArray value) {
            return value.asObjectCopy();
        }

        @Override
        public Number mapNumber(NumberValue value) {
            return value.asObject();
        }

        @Override
        public byte[] mapByteArray(ByteArray value) {
            return value.asObjectCopy();
        }

        @Override
        public short[] mapShortArray(ShortArray value) {
            return value.asObjectCopy();
        }

        @Override
        public int[] mapIntArray(IntArray value) {
            return value.asObjectCopy();
        }

        @Override
        public long[] mapLongArray(LongArray value) {
            return value.asObjectCopy();
        }

        @Override
        public float[] mapFloatArray(FloatArray value) {
            return value.asObjectCopy();
        }

        @Override
        public double[] mapDoubleArray(DoubleArray value) {
            return value.asObjectCopy();
        }

        @Override
        public ZonedDateTime mapDateTime(DateTimeValue value) {
            return value.asObjectCopy();
        }

        @Override
        public LocalDateTime mapLocalDateTime(LocalDateTimeValue value) {
            return value.asObjectCopy();
        }

        @Override
        public LocalDate mapDate(DateValue value) {
            return value.asObjectCopy();
        }

        @Override
        public OffsetTime mapTime(TimeValue value) {
            return value.asObjectCopy();
        }

        @Override
        public LocalTime mapLocalTime(LocalTimeValue value) {
            return value.asObjectCopy();
        }

        @Override
        public TemporalAmount mapDuration(DurationValue value) {
            return value.asObjectCopy();
        }
    }
}
