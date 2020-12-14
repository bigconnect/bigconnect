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
    DATE("date"),
    STRING("string"),
    GEO_LOCATION("geoLocation"),
    GEO_SHAPE("geoshape"),
    IMAGE("image"),
    BINARY("binary"),
    CURRENCY("currency"),
    DOUBLE("double"),
    BOOLEAN("boolean"),
    INTEGER("integer"),
    DIRECTORY_ENTITY("directory/entity"),
    EXTENDED_DATA_TABLE("extendedDataTable"),
    UNKNOWN("unknown");

    public static final String GE_TYPE_GEO_POINT = "com.mware.ge.type.GeoPoint";
    public static final String GE_TYPE_GEO_SHAPE = "com.mware.ge.type.GeoShape";

    private final String text;

    PropertyType(String text) {
        this.text = text;
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

    public static Class<? extends Value> getTypeClass(PropertyType propertyType) {
        switch (propertyType) {
            case DATE:
                return DateTimeValue.class;
            case STRING:
            case DIRECTORY_ENTITY:
                return StringValue.class;
            case GEO_LOCATION:
                return GeoPointValue.class;
            case GEO_SHAPE:
                return GeoShapeValue.class;
            case EXTENDED_DATA_TABLE:
                return StringArray.class;
            case IMAGE:
            case BINARY:
                return ByteValue.class;
            case CURRENCY:
                return FloatValue.class;
            case BOOLEAN:
                return BooleanValue.class;
            case DOUBLE:
                return DoubleValue.class;
            case INTEGER:
                return LongValue.class;
            case UNKNOWN:
                return TextValue.class;
            default:
                throw new RuntimeException("Unhandled property type: " + propertyType);
        }
    }
}
