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
package com.mware.core.model.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.ClientApiSchema;
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.properties.types.*;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.type.*;
import com.mware.ge.values.TemporalParseException;
import com.mware.ge.values.storable.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;

import java.text.ParseException;
import java.time.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class SchemaProperty {
    public static final Pattern GEO_LOCATION_ALTERNATE_FORMAT = Pattern.compile("([0-9\\.]*)\\s*,\\s*([0-9\\.]*)", Pattern.CASE_INSENSITIVE);
    public static final Pattern GEO_CIRCLE_FORMAT = Pattern.compile("CIRCLE\\s*\\(\\s*([0-9\\.]*)\\s*([0-9\\.]*)\\s*([0-9\\.]*)\\s*\\)", Pattern.CASE_INSENSITIVE);
    public static final Pattern GEO_RECT_FORMAT = Pattern.compile("RECT\\s*\\(\\s*\\(\\s*([0-9\\.]*)\\s*([0-9\\.]*)\\s*\\)\\s*\\(\\s*([0-9\\.]*)\\s*([0-9\\.]*)\\s*\\)\\s*\\)", Pattern.CASE_INSENSITIVE);

    public abstract String getId();

    public abstract SandboxStatus getSandboxStatus();

    public abstract String getName();

    public abstract String getDisplayName();

    public abstract boolean getUserVisible();

    public abstract boolean getSearchFacet();

    public abstract boolean getSystemProperty();

    public abstract String getAggType();

    public abstract int getAggPrecision();

    public abstract String getAggInterval();

    public abstract long getAggMinDocumentCount();

    public abstract String getAggTimeZone();

    public abstract String getAggCalendarField();

    public abstract boolean getSearchable();

    public abstract boolean getAddable();

    public abstract boolean getSortable();

    public abstract PropertyType getDataType();

    public abstract Double getBoost();

    public abstract Map<String, String> getPossibleValues();

    public abstract String getDisplayType();

    public abstract String getPropertyGroup();

    public abstract String getValidationFormula();

    public abstract String getDisplayFormula();

    public abstract boolean getUpdateable();

    public abstract boolean getDeleteable();

    public abstract ImmutableList<String> getDependentPropertyNames();

    public abstract String[] getIntents();

    public abstract String[] getTextIndexHints();

    public abstract void addTextIndexHints(String textIndexHints, Authorizations authorizations);

    public abstract void addIntent(String intent, Authorizations authorizations);

    public abstract void removeIntent(String intent, Authorizations authorizations);

    public abstract List<String> getConceptNames();

    public abstract List<String> getRelationshipNames();

    public abstract Integer getSortPriority();

    public void updateIntents(String[] newIntents, Authorizations authorizations) {
        ArrayList<String> toBeRemovedIntents = Lists.newArrayList(getIntents());
        for (String newIntent : newIntents) {
            if (toBeRemovedIntents.contains(newIntent)) {
                toBeRemovedIntents.remove(newIntent);
            } else {
                addIntent(newIntent, authorizations);
            }
        }
        for (String toBeRemovedIntent : toBeRemovedIntents) {
            removeIntent(toBeRemovedIntent, authorizations);
        }
    }

    public abstract void setProperty(String name, Value value, User user, Authorizations authorizations);

    public static Collection<ClientApiSchema.Property> toClientApiProperties(Iterable<SchemaProperty> properties) {
        Collection<ClientApiSchema.Property> results = new ArrayList<>();
        for (SchemaProperty property : properties) {
            results.add(property.toClientApi());
        }
        return results;
    }

    public abstract Map<String, String> getMetadata();

    public ClientApiSchema.Property toClientApi() {
        try {
            ClientApiSchema.Property result;
            if (this instanceof ExtendedDataTableProperty) {
                result = new ClientApiSchema.ExtendedDataTableProperty();
                ExtendedDataTableProperty edtp = (ExtendedDataTableProperty) this;
                ClientApiSchema.ExtendedDataTableProperty cedtp = (ClientApiSchema.ExtendedDataTableProperty) result;
                cedtp.setTitleFormula(edtp.getTitleFormula());
                cedtp.setSubtitleFormula(edtp.getSubtitleFormula());
                cedtp.setTimeFormula(edtp.getTimeFormula());
                cedtp.setTablePropertyNames(edtp.getTablePropertyNames());
            } else {
                result = new ClientApiSchema.Property();
            }
            result.setTitle(getName());
            result.setDisplayName(getDisplayName());
            result.setUserVisible(getUserVisible());
            result.setSearchFacet(getSearchFacet());
            result.setSystemProperty(getSystemProperty());
            result.setAggType(getAggType());
            result.setAggPrecision(getAggPrecision());
            result.setAggInterval(getAggInterval());
            result.setAggMinDocumentCount(getAggMinDocumentCount());
            result.setAggTimeZone(getAggTimeZone());
            result.setAggCalendarField(getAggCalendarField());
            result.setSearchable(getSearchable());
            result.setAddable(getAddable());
            result.setSortable(getSortable());
            result.setDataType(getDataType());
            result.setDisplayType(getDisplayType());
            result.setPropertyGroup(getPropertyGroup());
            result.setValidationFormula(getValidationFormula());
            result.setDisplayFormula(getDisplayFormula());
            result.setDependentPropertyNames(getDependentPropertyNames());
            result.setDeleteable(getDeleteable());
            result.setUpdateable(getUpdateable());
            result.setSandboxStatus(getSandboxStatus());
            result.setSortPriority(getSortPriority());
            if (getPossibleValues() != null) {
                result.getPossibleValues().putAll(getPossibleValues());
            }
            if (getIntents() != null) {
                result.getIntents().addAll(Arrays.asList(getIntents()));
            }
            if (getTextIndexHints() != null) {
                result.getTextIndexHints().addAll(Arrays.asList(getTextIndexHints()));
            }
            for (Map.Entry<String, String> additionalProperty : getMetadata().entrySet()) {
                result.getMetadata().put(additionalProperty.getKey(), additionalProperty.getValue());
            }
            return result;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public Value convertString(String valueStr) {
        PropertyType dataType = getDataType();
        char ARRAY_SEPARATOR = '|';

        switch (dataType) {
            case BOOLEAN:
                return Values.booleanValue(Boolean.parseBoolean(valueStr));
            case BOOLEAN_ARRAY:
                Boolean[] boolArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(Boolean::parseBoolean)
                        .toArray(Boolean[]::new);
                return Values.booleanArray(ArrayUtils.toPrimitive(boolArr, false));
            case BYTE:
                return Values.byteValue(Byte.parseByte(valueStr));
            case BYTE_ARRAY:
                Byte[] byteArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(Byte::parseByte)
                        .toArray(Byte[]::new);
                return Values.byteArray(ArrayUtils.toPrimitive(byteArr, (byte) 0));
            case CHAR:
                return Values.charValue(valueStr.charAt(0));
            case CHAR_ARRAY:
                Character[] charArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(s -> s.charAt(0))
                        .toArray(Character[]::new);
                return Values.charArray(ArrayUtils.toPrimitive(charArr, '\0'));
            case DATETIME:
                return parseDateTime(valueStr);
            case DATETIME_ARRAY:
                ZonedDateTime[] dateArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(SchemaProperty::parseDateTime)
                        .map(DateTimeValue::asObjectCopy)
                        .toArray(ZonedDateTime[]::new);
                return Values.dateTimeArray(dateArr);
            case LOCAL_DATETIME:
                return parseLocalDateTime(valueStr);
            case LOCAL_DATETIME_ARRAY:
                LocalDateTime[] localDateTimeArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(SchemaProperty::parseLocalDateTime)
                        .map(LocalDateTimeValue::asObjectCopy)
                        .toArray(LocalDateTime[]::new);
                return Values.localDateTimeArray(localDateTimeArr);
            case LOCAL_DATE:
                return parseLocalDate(valueStr);
            case LOCAL_DATE_ARRAY:
                LocalDate[] localDateArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(SchemaProperty::parseLocalDate)
                        .map(DateValue::asObjectCopy)
                        .toArray(LocalDate[]::new);
                return Values.dateArray(localDateArr);
            case STRING:
                return Values.stringValue(valueStr);
            case STRING_ARRAY:
                String[] strArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .toArray(String[]::new);
                return Values.stringArray(strArr);
            case DOUBLE:
                return Values.doubleValue(Double.parseDouble(valueStr));
            case DOUBLE_ARRAY:
                Double[] dblArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(Double::parseDouble)
                        .toArray(Double[]::new);
                return Values.doubleArray(ArrayUtils.toPrimitive(dblArr, 0));
            case DURATION:
                try {
                    return DurationValue.parse(valueStr);
                } catch (Exception ex) {
                    return DurationValue.duration(Duration.ofSeconds(Long.parseLong(valueStr)));
                }
            case DURATION_ARRAY:
                DurationValue[] durArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(DurationValue::parse)
                        .toArray(DurationValue[]::new);
                return Values.durationArray(durArr);
            case FLOAT:
                return Values.floatValue(Float.parseFloat(valueStr));
            case FLOAT_ARRAY:
                Float[] floatArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(Float::parseFloat)
                        .toArray(Float[]::new);
                return Values.floatArray(ArrayUtils.toPrimitive(floatArr, (float) 0));
            case GEO_CIRCLE:
                return parseGeoCircle(valueStr);
            case GEO_LINE:
                return parseGeoLine(valueStr);
            case GEO_RECT:
                return parseGeoRect(valueStr);
            case GEO_POLYGON:
                return parseGeoPolygon(valueStr);
            case GEO_POINT:
            case GEO_LOCATION:
                return parseGeoLocation(valueStr);
            case INTEGER:
                return Values.intValue(Integer.parseInt(valueStr));
            case INTEGER_ARRAY:
                Integer[] intArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(Integer::parseInt)
                        .toArray(Integer[]::new);
                return Values.intArray(ArrayUtils.toPrimitive(intArr,  0));
            case SHORT:
                return Values.shortValue(Short.parseShort(valueStr));
            case SHORT_ARRAY:
                Short[] shortArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(Short::parseShort)
                        .toArray(Short[]::new);
                return Values.shortArray(ArrayUtils.toPrimitive(shortArr,  (short) 0));
            case LONG:
                return Values.longValue(Long.parseLong(valueStr));
            case LONG_ARRAY:
                Long[] longArr = Arrays.stream(StringUtils.split(valueStr, ARRAY_SEPARATOR))
                        .map(String::trim)
                        .map(Long::parseLong)
                        .toArray(Long[]::new);
                return Values.longArray(ArrayUtils.toPrimitive(longArr, 0L));
        }
        return Values.stringValue(valueStr);
    }

    public static Value convert(JSONArray values, PropertyType propertyDataType, int index) throws ParseException {
        switch (propertyDataType) {
            case DATETIME: {
                String valueStr = values.getString(index);
                return parseDateTime(valueStr);
            }
            case LOCAL_DATE: {
                String valueStr = values.getString(index);
                return parseLocalDate(valueStr);
            }
            case LOCAL_DATETIME: {
                String valueStr = values.getString(index);
                return parseLocalDateTime(valueStr);
            }
            case GEO_POINT:
            case GEO_LOCATION:
                if (values.get(index) instanceof String) {
                    String valueStr = values.getString(index);
                    return Values.geoHashValue(new GeoHash(valueStr));
                }
                return Values.geoCircleValue(new GeoCircle(
                        values.getDouble(index),
                        values.getDouble(index + 1),
                        values.getDouble(index + 2)
                ));
            case FLOAT:
                return Values.floatValue(Float.parseFloat(values.getString(index)));
            case INTEGER:
                return Values.intValue(values.getInt(index));
            case DOUBLE:
                return Values.doubleValue(values.getDouble(index));
            case LONG:
                return Values.longValue(values.getLong(index));
            case BYTE:
                return Values.byteValue((byte) values.getInt(index));
            case CHAR:
                return Values.charValue((char) values.getInt(index));
            case SHORT:
                return Values.shortValue((short) values.getInt(index));
            case BOOLEAN:
                Object result = values.get(index);
                if ("T".equals(result) || "1".equals(result)) {
                    return BooleanValue.TRUE;
                }
                if ("F".equals(result) || "0".equals(result)) {
                    return BooleanValue.FALSE;
                }
                return Values.booleanValue(values.getBoolean(index));
        }
        return Values.stringValue(values.getString(index));
    }

    protected static GeoRectValue parseGeoRect(String valueStr) {
        Matcher match = GEO_RECT_FORMAT.matcher(valueStr);
        if (match.find()) {
            double p1x = Double.parseDouble(match.group(1).trim());
            double p1y = Double.parseDouble(match.group(2).trim());
            double p2x = Double.parseDouble(match.group(3).trim());
            double p2y = Double.parseDouble(match.group(4).trim());
            return Values.geoRectValue(new GeoPoint(p1x, p1y), new GeoPoint(p2x, p2y));
        }
        throw new BcException("Could not parse GeoRect: " + valueStr);
    }

    protected static GeoPolygonValue parseGeoPolygon(String valueStr) {
        try {
            Polygon polygon = (Polygon) new WKTReader().read(valueStr);
            List<GeoPoint> boundary = Arrays.stream(polygon.getExteriorRing().getCoordinates())
                    .map(c -> new GeoPoint(c.x, c.y))
                    .collect(Collectors.toList());
            Collections.reverse(boundary);

            List<List<GeoPoint>> interiorRings = new ArrayList<>();
            for (int i=0; i<polygon.getNumInteriorRing(); i++) {
                interiorRings.add(Arrays.stream(polygon.getInteriorRingN(i).getCoordinates())
                        .map(c -> new GeoPoint(c.x, c.y))
                        .collect(Collectors.toList()));
            }
            return Values.geoPolygonValue(new GeoPolygon(boundary, interiorRings));
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new BcException("Could not parse GeoLine: " + valueStr+": "+e.getMessage());
        }
    }

    protected static GeoLineValue parseGeoLine(String valueStr) {
        try {
            LineString line = (LineString) new WKTReader().read(valueStr);
            GeoPoint[] geoPoints = Arrays.stream(line.getCoordinates())
                    .map(c -> new GeoPoint(c.x, c.y))
                    .toArray(GeoPoint[]::new);
            return Values.geoLineValue(new GeoLine(geoPoints));
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new BcException("Could not parse GeoLine: " + valueStr+": "+e.getMessage());
        }
    }

    protected static GeoCircleValue parseGeoCircle(String valueStr) {
        Matcher match = GEO_CIRCLE_FORMAT.matcher(valueStr);
        if (match.find()) {
            double latitude = Double.parseDouble(match.group(1).trim());
            double longitude = Double.parseDouble(match.group(2).trim());
            double radius = Double.parseDouble(match.group(3).trim());
            return Values.geoCircleValue(latitude, longitude, radius);
        }
        throw new BcException("Could not parse GeoCircle: " + valueStr);
    }

    protected static GeoPointValue parseGeoLocation(String valueStr) {
        try {
            JSONObject json = new JSONObject(valueStr);
            double latitude = json.getDouble("latitude");
            double longitude = json.getDouble("longitude");
            String altitudeString = json.optString("altitude");
            double altitude = (altitudeString == null || altitudeString.length() == 0) ? 0.0d : Double.parseDouble(altitudeString);
            return Values.geoPointValue(new GeoPoint(latitude, longitude, altitude));
        } catch (Exception ex) {
            try {
                Point point = (Point) new WKTReader().read(valueStr);
                return Values.geoPointValue(point.getX(), point.getY());
            } catch (org.locationtech.jts.io.ParseException e) {
                Matcher match = GEO_LOCATION_ALTERNATE_FORMAT.matcher(valueStr);
                if (match.find()) {
                    double latitude = Double.parseDouble(match.group(1).trim());
                    double longitude = Double.parseDouble(match.group(2).trim());
                    return Values.geoPointValue(latitude, longitude);
                }
            }
        }

        throw new BcException("Could not parse GeoLocation: " + valueStr);
    }

    public boolean hasDependentPropertyNames() {
        return getDependentPropertyNames() != null && getDependentPropertyNames().size() > 0;
    }

    private static DateTimeValue parseDateTime(String valueStr) {
        try {
            return DateTimeValue.parse(valueStr, () -> ZoneOffset.UTC);
        } catch (TemporalParseException ex) {
            return DateTimeValue.ofEpochMillis(Values.longValue(Long.parseLong(valueStr)));
        }
    }

    private static LocalDateTimeValue parseLocalDateTime(String valueStr) {
        try {
            return LocalDateTimeValue.parse(valueStr);
        } catch (TemporalParseException ex) {
            return LocalDateTimeValue.localDateTime(Long.parseLong(valueStr), 0L);
        }
    }

    private static DateValue parseLocalDate(String valueStr) {
        try {
            return DateValue.parse(valueStr);
        } catch (TemporalParseException ex) {
            return DateValue.epochDate(Long.parseLong(valueStr));
        }
    }

    public BcProperty geBcProperty() {
        switch (getDataType()) {
            case BOOLEAN:
                return new BooleanBcProperty(getName());
            case BOOLEAN_ARRAY:
                return new BooleanArrayBcProperty(getName());
            case BYTE:
                return new ByteBcProperty(getName());
            case BYTE_ARRAY:
                return new ByteArrayBcProperty(getName());
            case CHAR:
                return new CharBcProperty(getName());
            case CHAR_ARRAY:
                return new CharArrayBcProperty(getName());
            case DATETIME_ARRAY:
                return new DateTimeArrayBcProperty(getName());
            case DATETIME:
                return new DateTimeBcProperty(getName());
            case LOCAL_DATE:
                return new LocalDateBcProperty(getName());
            case LOCAL_DATE_ARRAY:
                return new LocalDateArrayBcProperty(getName());
            case LOCAL_DATETIME:
                return new LocalDateTimeBcProperty(getName());
            case LOCAL_DATETIME_ARRAY:
                return new LocalDateTimeArrayBcProperty(getName());
            case STRING:
                return new StringBcProperty(getName());
            case STRING_ARRAY:
                return new StringArrayBcProperty(getName());
            case DOUBLE:
                return new DoubleBcProperty(getName());
            case DOUBLE_ARRAY:
                return new DoubleArrayBcProperty(getName());
            case FLOAT:
                return new FloatBcProperty(getName());
            case FLOAT_ARRAY:
                return new FloatArrayBcProperty(getName());
            case INTEGER:
                return new IntegerBcProperty(getName());
            case INTEGER_ARRAY:
                return new IntegerArrayBcProperty(getName());
            case LONG:
                return new LongBcProperty(getName());
            case LONG_ARRAY:
                return new LongArrayBcProperty(getName());
            case DURATION:
                return new DurationBcProperty(getName());
            case DURATION_ARRAY:
                return new DurationArrayBcProperty(getName());
            case GEO_CIRCLE:
                return new GeoCircleBcProperty(getName());
            case GEO_LINE:
                return new GeoLineBcProperty(getName());
            case GEO_RECT:
                return new GeoRectBcProperty(getName());
            case GEO_LOCATION:
            case GEO_POINT:
                return new GeoPointBcProperty(getName());
            case GEO_POLYGON:
                return new GeoPolygonBcProperty(getName());
            case STREAMING:
                return new StreamingBcProperty(getName());
            case SHORT:
                return new ShortBcProperty(getName());
            case SHORT_ARRAY:
                return new ShortArrayBcProperty(getName());
            default:
                throw new BcException("Could not get " + BcProperty.class.getName() + " for data type " + getDataType());
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{name:" + getName() + "}";
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SchemaProperty)) {
            return false;
        }

        String otherName = ((SchemaProperty) obj).getName();
        return getName().equals(otherName);
    }
}
