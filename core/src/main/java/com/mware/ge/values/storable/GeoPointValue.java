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

import com.mware.ge.csv.CSVHeaderInformation;
import com.mware.ge.hashing.HashFunction;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.InvalidValuesArgumentException;
import com.mware.ge.values.ValueMapper;
import com.mware.ge.values.virtual.MapValue;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class GeoPointValue extends GeoShapeValue implements Comparable<GeoPointValue> {
    public static final GeoPointValue MIN_VALUE_CARTESIAN = new GeoPointValue(new GeoPoint(Double.MIN_VALUE, Double.MIN_VALUE));
    public static final GeoPointValue MAX_VALUE_CARTESIAN = new GeoPointValue(new GeoPoint(Double.MAX_VALUE, Double.MAX_VALUE));
    public static final GeoPointValue MIN_VALUE_WGS84 = new GeoPointValue(new GeoPoint(-180D, -90));
    public static final GeoPointValue MAX_VALUE_WGS84 = new GeoPointValue(new GeoPoint(180D, 90));

    GeoPointValue(GeoPoint geoPoint) {
       super(geoPoint);
    }

    public static AnyValue of(String str) {
        return new GeoPointValue(GeoPoint.parse(str));
    }

    @Override
    public int compareTo(GeoPointValue o) {
        return ((GeoPoint) geoShape).compareTo(((GeoPoint) o.geoShape));
    }

    @Override
    int unsafeCompareTo(Value otherValue) {
        return compareTo((GeoPointValue) otherValue);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeGeoPoint(((GeoPoint) geoShape));
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapGeoPoint(this);
    }

    @Override
    public String getTypeName() {
        return "GeoPointValue";
    }

    public DoubleValue getLongitude() {
        return Values.doubleValue(((GeoPoint) geoShape).getLongitude());
    }

    public DoubleValue getLatitude() {
        return Values.doubleValue(((GeoPoint) geoShape).getLatitude());
    }

    public DoubleValue getAltitude() {
        return Values.doubleValue(((GeoPoint) geoShape).getAltitude());
    }

    /**
     * For accessors from cypher.
     */
    public Value get(String fieldName) {
        return GeoPointFields.fromName(fieldName).get(this);
    }

    public static GeoPointValue fromMap(MapValue map) {
        GeoPointBuilder fields = new GeoPointBuilder();
        map.foreach((key, value) -> fields.assign(key.toLowerCase(), value));
        return fromInputFields(fields);
    }

    private static GeoPointValue fromInputFields(GeoPointBuilder fields) {
        double[] coordinates;
        if (fields.x != null && fields.y != null) {
            coordinates = fields.z != null ? new double[]{fields.x, fields.y, fields.z} : new double[]{fields.x, fields.y};
        } else if (fields.latitude != null && fields.longitude != null) {
            if (fields.z != null) {
                coordinates = new double[]{fields.longitude, fields.latitude, fields.z};
            } else if (fields.height != null) {
                coordinates = new double[]{fields.longitude, fields.latitude, fields.height};
            } else {
                coordinates = new double[]{fields.longitude, fields.latitude};
            }
        } else {
            throw new InvalidValuesArgumentException("A point must contain either 'x' and 'y' or 'latitude' and 'longitude'");
        }

        return Values.geoPointValue(coordinates[1], coordinates[0]);
    }

    private static class GeoPointBuilder implements CSVHeaderInformation {
        private String crs;
        private Double x;
        private Double y;
        private Double z;
        private Double longitude;
        private Double latitude;
        private Double height;
        private int srid = -1;

        @Override
        public void assign(String key, Object value) {
            switch (key.toLowerCase()) {
                case "crs":
                    checkUnassigned(crs, key);
                    assignTextValue(key, value, str -> crs = quotesPattern.matcher(str).replaceAll(""));
                    break;
                case "x":
                    checkUnassigned(x, key);
                    assignFloatingPoint(key, value, i -> x = i);
                    break;
                case "y":
                    checkUnassigned(y, key);
                    assignFloatingPoint(key, value, i -> y = i);
                    break;
                case "z":
                    checkUnassigned(z, key);
                    assignFloatingPoint(key, value, i -> z = i);
                    break;
                case "longitude":
                    checkUnassigned(longitude, key);
                    assignFloatingPoint(key, value, i -> longitude = i);
                    break;
                case "latitude":
                    checkUnassigned(latitude, key);
                    assignFloatingPoint(key, value, i -> latitude = i);
                    break;
                case "height":
                    checkUnassigned(height, key);
                    assignFloatingPoint(key, value, i -> height = i);
                    break;
                case "srid":
                    if (srid != -1) {
                        throw new InvalidValuesArgumentException(String.format("Duplicate field '%s' is not allowed.", key));
                    }
                    assignIntegral(key, value, i -> srid = i);
                    break;
                default:
            }
        }

        void mergeWithHeader(GeoPointBuilder header) {
            this.crs = this.crs == null ? header.crs : this.crs;
            this.x = this.x == null ? header.x : this.x;
            this.y = this.y == null ? header.y : this.y;
            this.z = this.z == null ? header.z : this.z;
            this.longitude = this.longitude == null ? header.longitude : this.longitude;
            this.latitude = this.latitude == null ? header.latitude : this.latitude;
            this.height = this.height == null ? header.height : this.height;
            this.srid = this.srid == -1 ? header.srid : this.srid;
        }

        private void assignTextValue(String key, Object value, Consumer<String> assigner) {
            if (value instanceof String) {
                assigner.accept((String) value);
            } else if (value instanceof TextValue) {
                assigner.accept(((TextValue) value).stringValue());
            } else {
                throw new InvalidValuesArgumentException(String.format("Cannot assign %s to field %s", value, key));
            }
        }

        private void assignFloatingPoint(String key, Object value, Consumer<Double> assigner) {
            if (value instanceof String) {
                assigner.accept(assertConvertible(() -> Double.parseDouble((String) value)));
            } else if (value instanceof IntegralValue) {
                assigner.accept(((IntegralValue) value).doubleValue());
            } else if (value instanceof FloatingPointValue) {
                assigner.accept(((FloatingPointValue) value).doubleValue());
            } else {
                throw new InvalidValuesArgumentException(String.format("Cannot assign %s to field %s", value, key));
            }
        }

        private void assignIntegral(String key, Object value, Consumer<Integer> assigner) {
            if (value instanceof String) {
                assigner.accept(assertConvertible(() -> Integer.parseInt((String) value)));
            } else if (value instanceof IntegralValue) {
                assigner.accept((int) ((IntegralValue) value).longValue());
            } else {
                throw new InvalidValuesArgumentException(String.format("Cannot assign %s to field %s", value, key));
            }
        }

        private <T extends Number> T assertConvertible(Supplier<T> func) {
            try {
                return func.get();
            } catch (NumberFormatException e) {
                throw new InvalidValuesArgumentException(e.getMessage(), e);
            }
        }

        private void checkUnassigned(Object key, String fieldName) {
            if (key != null) {
                throw new InvalidValuesArgumentException(String.format("Duplicate field '%s' is not allowed.", fieldName));
            }
        }
    }

    public static GeoPointBuilder parseHeaderInformation(CharSequence text) {
        GeoPointBuilder fields = new GeoPointBuilder();
        Value.parseHeaderInformation(text, "point", fields);
        return fields;
    }

    /**
     * Parses the given text into a PointValue. The information stated in the header is saved into the PointValue
     * unless it is overridden by the information in the text
     *
     * @param text             the input text to be parsed into a PointValue
     * @param fieldsFromHeader must be a value obtained from {@link #parseHeaderInformation(CharSequence)} or null
     * @return a PointValue instance with information from the {@param fieldsFromHeader} and {@param text}
     */
    public static GeoPointValue parse(CharSequence text, CSVHeaderInformation fieldsFromHeader) {
        GeoPointBuilder fieldsFromData = parseHeaderInformation(text);
        if (fieldsFromHeader != null) {
            // Merge InputFields: Data fields override header fields
            if (!(fieldsFromHeader instanceof GeoPointBuilder)) {
                throw new IllegalStateException("Wrong header information type: " + fieldsFromHeader);
            }
            fieldsFromData.mergeWithHeader((GeoPointBuilder) fieldsFromHeader);
        }
        return fromInputFields(fieldsFromData);
    }
}
