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
 *
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
package com.mware.core.ingest.structured.mapping;

import com.mware.core.exception.BcException;
import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.security.VisibilityTranslator;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GeoPointPropertyMapping extends PropertyMapping {
    public static final String PROPERTY_MAPPING_FORMAT_KEY = "format";
    public static final String PROPERTY_MAPPING_COLUMN_LAT_KEY = "columnLatitude";
    public static final String PROPERTY_MAPPING_COLUMN_LON_KEY = "columnLongitude";

    private static final String DECIMAL_NUMBER_REGEX = "(-?\\d*\\.?\\d+)";
    private static final String INTEGER_NUMBER_REGEX = "(-?\\d+)";
    private static final String DEGREES_REGEX = DECIMAL_NUMBER_REGEX + "\\D*?";
    private static final String DEGREES_MINUTES_REGEX = INTEGER_NUMBER_REGEX + "\\D+" + DECIMAL_NUMBER_REGEX + "\\D*?";
    private static final String DEGREES_MINUTES_SECONDS_REGEX = INTEGER_NUMBER_REGEX + "\\D+" + INTEGER_NUMBER_REGEX + "\\D+" + DECIMAL_NUMBER_REGEX + "\\D*?";

    public String latColumn;
    public String lonColumn;

    public enum Format {
        DECIMAL(DEGREES_REGEX + "[^\\d\\-]+" + DEGREES_REGEX),
        DEGREES_DECIMAL_MINUTES(DEGREES_MINUTES_REGEX + "[^\\d\\-]+" + DEGREES_MINUTES_REGEX),
        DEGREES_MINUTES_SECONDS(DEGREES_MINUTES_SECONDS_REGEX + "[^\\d\\-]+" + DEGREES_MINUTES_SECONDS_REGEX);

        private Pattern formatPattern;

        Format(String regex) {
            formatPattern = Pattern.compile(regex);
        }

        public double[] parse(String input) {
            Matcher matcher = formatPattern.matcher(input);
            if(matcher.matches()) {
                int totalGroupCount = matcher.groupCount();
                int componentGroupCount = totalGroupCount/2;

                return new double[] {
                        parseDouble(0, componentGroupCount, matcher),
                        parseDouble(1, componentGroupCount, matcher)
                };
            }
            throw new BcException("Unrecognized geo point format: " + input);
        }

        private double parseDouble(int index, int componentGroupCount, Matcher matcher) {
            int offset = index * componentGroupCount + 1;

            double result = Double.parseDouble(matcher.group(offset));
            if(componentGroupCount > 1) {
                result += (result < 0 ? -1 : 1) * Double.parseDouble(matcher.group(offset + 1)) / 60;
            }
            if(componentGroupCount > 2) {
                result += (result < 0 ? -1 : 1) * Double.parseDouble(matcher.group(offset + 2)) / 3600;
            }
            return result;
        }
    }

    public Format format;

    public GeoPointPropertyMapping() {
    }

    public GeoPointPropertyMapping(
            VisibilityTranslator visibilityTranslator, String workspaceId, JSONObject propertyMapping) {
        super(visibilityTranslator, workspaceId, propertyMapping);

        format = Format.valueOf(propertyMapping.getString(PROPERTY_MAPPING_FORMAT_KEY));

        latColumn = propertyMapping.optString(PROPERTY_MAPPING_COLUMN_LAT_KEY);
        lonColumn = propertyMapping.optString(PROPERTY_MAPPING_COLUMN_LON_KEY);

        if(Arrays.asList(value, key, latColumn, lonColumn).stream().allMatch(StringUtils::isBlank)){
            throw new BcException("You must provide one of: value, column, or latColumn/lonColumn");
        }
    }

    public GeoPointPropertyMapping(VisibilityTranslator visibilityTranslator, String workspaceId, ClientApiDataSource.EntityMapping mapping) {
        super(visibilityTranslator, workspaceId, mapping);

    }

    @Override
    public String extractRawValue(Map<String, Object> row) {
        if(StringUtils.isNotBlank(key) || !StringUtils.isBlank(value)) {
            return super.extractRawValue(row).toString();
        }

        String latRawValue = (String) row.get(latColumn);
        String lonRawValue = (String) row.get(lonColumn);

        if(StringUtils.isBlank(latRawValue) || StringUtils.isBlank(lonRawValue)) {
            return null;
        }

        return latRawValue.trim() + ", " + lonRawValue.trim();
    }

    @Override
    public Value decodeValue(Object rawPropertyValue) {
        if(rawPropertyValue instanceof String && !StringUtils.isBlank((String) rawPropertyValue)) {
            try {
                double[] latLon = format.parse((String) rawPropertyValue);
                return Values.geoPointValue(new GeoPoint(latLon[0], latLon[1]));
            }  catch (BcException ex) {
                return Values.geoPointValue(GeoPoint.parse((String) rawPropertyValue));
            }
        }
        return null;
    }
}
