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
import com.mware.core.model.properties.types.*;
import com.mware.core.user.User;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.ge.type.GeoShape;
import com.mware.ge.values.storable.*;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.mware.ge.Authorizations;
import com.mware.ge.type.GeoCircle;
import com.mware.ge.type.GeoHash;
import com.mware.ge.type.GeoPoint;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.ClientApiSchema;
import com.mware.core.model.clientapi.dto.PropertyType;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class SchemaProperty {
    public static final DateTimeFormatter DATE_FORMAT;
    public static final DateTimeFormatter DATE_TIME_FORMAT;
    public static final DateTimeFormatter DATE_TIME_WITH_SECONDS_FORMAT;
    public static final Pattern GEO_LOCATION_FORMAT = Pattern.compile("POINT\\((.*?),(.*?)\\)", Pattern.CASE_INSENSITIVE);
    public static final Pattern GEO_LOCATION_ALTERNATE_FORMAT = Pattern.compile("(.*?),(.*)", Pattern.CASE_INSENSITIVE);

    static {
        DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
        DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm z").withZone(ZoneOffset.UTC);
        DATE_TIME_WITH_SECONDS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(ZoneOffset.UTC);
    }
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

    public Object convert(Object value) throws ParseException {
        if (value == null) {
            return null;
        }

        PropertyType dataType = getDataType();
        switch (dataType) {
            case DATE:
                if (value instanceof Date) {
                    return value;
                }
                break;
            case GEO_LOCATION:
                if (value instanceof GeoPoint) {
                    return value;
                }
                break;
            case GEO_SHAPE:
                if (value instanceof GeoShape) {
                    return value;
                }
                break;
            case CURRENCY:
                if (value instanceof BigDecimal) {
                    return value;
                }
                break;
            case DOUBLE:
                if (value instanceof Double) {
                    return value;
                }
                break;
            case INTEGER:
                if (value instanceof Integer) {
                    return value;
                }
                break;
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                break;
        }
        return convertString(value.toString());
    }

    public Value convertString(String valueStr) throws ParseException {
        PropertyType dataType = getDataType();
        switch (dataType) {
            case DATE:
                return parseDateTime(valueStr);
            case GEO_LOCATION:
                return parseGeoLocation(valueStr);
            case CURRENCY:
                return Values.floatValue(Float.parseFloat(valueStr));
            case DOUBLE:
                return Values.doubleValue(Double.parseDouble(valueStr));
            case INTEGER:
                return Values.intValue(Integer.parseInt(valueStr));
            case BOOLEAN:
                return Values.booleanValue(Boolean.parseBoolean(valueStr));
        }
        return Values.stringValue(valueStr);
    }

    public static Value convert(JSONArray values, PropertyType propertyDataType, int index) throws ParseException {
        switch (propertyDataType) {
            case DATE: {
                String valueStr = values.getString(index);
                return parseDateTime(valueStr);
            }
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
            case CURRENCY:
                return Values.floatValue(Float.parseFloat(values.getString(index)));
            case INTEGER:
                return Values.intValue(values.getInt(index));
            case DOUBLE:
                return Values.doubleValue(values.getDouble(index));
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

    protected static GeoPointValue parseGeoLocation(String valueStr) {
        try {
            JSONObject json = new JSONObject(valueStr);
            double latitude = json.getDouble("latitude");
            double longitude = json.getDouble("longitude");
            String altitudeString = json.optString("altitude");
            double altitude = (altitudeString == null || altitudeString.length() == 0) ? 0.0d : Double.parseDouble(altitudeString);
            return Values.geoPointValue(new GeoPoint(latitude, longitude, altitude));
        } catch (Exception ex) {
            Matcher match = GEO_LOCATION_FORMAT.matcher(valueStr);
            if (match.find()) {
                double latitude = Double.parseDouble(match.group(1).trim());
                double longitude = Double.parseDouble(match.group(2).trim());
                return Values.geoPointValue(latitude, longitude);
            }
            match = GEO_LOCATION_ALTERNATE_FORMAT.matcher(valueStr);
            if (match.find()) {
                double latitude = Double.parseDouble(match.group(1).trim());
                double longitude = Double.parseDouble(match.group(2).trim());
                return Values.geoPointValue(latitude, longitude);
            }
            throw new BcException("Could not parse location: " + valueStr);
        }
    }

    public boolean hasDependentPropertyNames() {
        return getDependentPropertyNames() != null && getDependentPropertyNames().size() > 0;
    }

    private static DateTimeValue parseDateTime(String valueStr) throws ParseException {
        ZonedDateTime date;

        try {
            date = ZonedDateTime.parse(valueStr, DATE_TIME_WITH_SECONDS_FORMAT);
        } catch (DateTimeParseException ex1) {
            try {
                date = ZonedDateTime.parse(valueStr, DATE_TIME_FORMAT);
            } catch (DateTimeParseException ex2) {
                try {
                    date = ZonedDateTime.parse(valueStr, DATE_FORMAT);
                } catch (DateTimeParseException ex3) {
                    return DateTimeValue.ofEpochMillis(Values.longValue(Long.parseLong(valueStr)));
                }
            }
        }


        return (DateTimeValue) Values.of(date);
    }

    public BcProperty geBcProperty() {
        switch (getDataType()) {
            case IMAGE:
            case BINARY:
                return new StreamingBcProperty(getName());
            case BOOLEAN:
                return new BooleanBcProperty(getName());
            case DATE:
                return new DateBcProperty(getName());
            case CURRENCY:
            case DOUBLE:
                return new DoubleBcProperty(getName());
            case GEO_LOCATION:
                return new GeoPointBcProperty(getName());
            case GEO_SHAPE:
                return new GeoShapeBcProperty(getName());
            case INTEGER:
                return new IntegerBcProperty(getName());
            case STRING:
            case DIRECTORY_ENTITY:
                return new StringBcProperty(getName());
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
