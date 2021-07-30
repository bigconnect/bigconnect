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
package com.mware.core.model.clientapi.dto;

import com.fasterxml.jackson.annotation.JsonValue;
import com.mware.ge.values.storable.*;

import java.math.BigDecimal;
import java.util.Date;

public enum PropertyType {
    BOOLEAN_ARRAY("booleanArray"),
    BOOLEAN("boolean"),
    BYTE_ARRAY("byteArray"),
    BYTE("byte"),
    CHAR_ARRAY("charArray"),
    CHAR("char"),
    DATE_ARRAY("dateArray"),
    DATE("date"),
    STRING_ARRAY("stringArray"),
    STRING("string"),
    DOUBLE_ARRAY("doubleArray"),
    DOUBLE("double"),
    DURATION_ARRAY("durationArray"),
    DURATION("duration"),
    FLOAT_ARRAY("floatArray"),
    FLOAT("float"),
    GEO_CIRCLE("geocircle"),
    GEO_LINE("geoline"),
    GEO_LOCATION("geoLocation"),
    GEO_POINT("geoLocation"),
    GEO_POLYGON("geopolygon"),
    GEO_RECT("georect"),
    INTEGER_ARRAY("integerArray"),
    INTEGER("integer"),
    SHORT_ARRAY("shortArray"),
    SHORT("short"),
    LONG_ARRAY("longArray"),
    LONG("long"),
    STREAMING("spv"),
    EXTENDED_DATA_TABLE("extendedDataTable");

    private final String text;

    PropertyType(String text) {
        this.text = text;
    }

    public static Class<? extends Value> getTypeClass(PropertyType propertyType) {
        switch (propertyType) {
            case BOOLEAN_ARRAY: return BooleanArray.class;
            case BOOLEAN: return BooleanValue.class;
            case BYTE_ARRAY: return ByteArray.class;
            case BYTE: return ByteValue.class;
            case CHAR_ARRAY: return CharArray.class;
            case CHAR: return CharValue.class;
            case DATE_ARRAY: return DateTimeArray.class;
            case DATE: return DateTimeValue.class;
            case STRING: return StringValue.class;
            case EXTENDED_DATA_TABLE:
            case STRING_ARRAY:
                return StringArray.class;
            case DOUBLE_ARRAY: return DoubleArray.class;
            case DOUBLE: return DoubleValue.class;
            case FLOAT_ARRAY: return FloatArray.class;
            case FLOAT: return FloatValue.class;
            case INTEGER: return IntValue.class;
            case INTEGER_ARRAY: return IntArray.class;
            case LONG: return LongValue.class;
            case LONG_ARRAY: return LongArray.class;
            case DURATION_ARRAY: return DurationArray.class;
            case DURATION: return DurationValue.class;
            case SHORT: return ShortValue.class;
            case SHORT_ARRAY: return ShortArray.class;
            case GEO_LOCATION:
            case GEO_POINT:
                return GeoPointValue.class;
            case GEO_RECT: return GeoRectValue.class;
            case GEO_CIRCLE: return GeoCircleValue.class;
            case GEO_LINE: return GeoLineValue.class;
            case GEO_POLYGON: return GeoPolygonValue.class;
            case STREAMING: return StreamingPropertyValue.class;
            default: throw new RuntimeException("Unhandled property type: " + propertyType);
        }
    }

    @Override
    public String toString() {
        return this.text;
    }

    @JsonValue
    public String getText() {
        return text;
    }

    public static PropertyType convert(String property) {
        return convert(property, STRING);
    }

    public static PropertyType convert(String property, PropertyType defaultValue) {
        for (PropertyType pt : PropertyType.values()) {
            if (pt.toString().equalsIgnoreCase(property)) {
                return pt;
            }
        }
        return defaultValue;
    }
}
