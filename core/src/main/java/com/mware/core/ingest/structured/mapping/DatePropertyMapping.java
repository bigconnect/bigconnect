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

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.security.VisibilityTranslator;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

public class DatePropertyMapping extends PropertyMapping {
    public static final String PROPERTY_MAPPING_DATE_FORMAT_KEY = "format";
    public static final String PROPERTY_MAPPING_DATE_TIMEZONE_KEY = "timezone";
    private static final String DEFAULT_FORMAT = "dd/MM/yy HH:mm";

    public SimpleDateFormat dateFormat;

    public DatePropertyMapping() {
    }

    public DatePropertyMapping(VisibilityTranslator visibilityTranslator, String workspaceId, JSONObject propertyMapping) {
        super(visibilityTranslator, workspaceId, propertyMapping);

        //if (propertyMapping.getBoolean("requiresTransform"))
        if (propertyMapping.has(PROPERTY_MAPPING_DATE_FORMAT_KEY)) {
            String format = propertyMapping.getString(PROPERTY_MAPPING_DATE_FORMAT_KEY);
            String timezone = propertyMapping.getString(PROPERTY_MAPPING_DATE_TIMEZONE_KEY);
            if (StringUtils.isBlank(format) || StringUtils.isBlank(timezone)) {
                throw new BcException("Both format and timezone are required for the Date propery " + name);
            }

            dateFormat = new SimpleDateFormat(format);
            dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
        }
    }

    public DatePropertyMapping(VisibilityTranslator visibilityTranslator, String workspaceId, ClientApiDataSource.EntityMapping mapping) {
        super(visibilityTranslator, workspaceId, mapping);

        if(!StringUtils.isEmpty(mapping.getColPropDateFormat())) {
            dateFormat = new SimpleDateFormat(mapping.getColPropDateFormat());
        }
    }

    @Override
    public Value decodeValue(Object rawPropertyValue) {
        if (rawPropertyValue instanceof String) {
            String strPropertyValue = (String) rawPropertyValue;
            if (StringUtils.isBlank(strPropertyValue)) {
                return null;
            } else {
                try {
                    if (dateFormat == null) {
                        dateFormat = new SimpleDateFormat(DEFAULT_FORMAT);
                    }
                    return DateTimeValue.ofEpochMillis(Values.longValue(dateFormat.parse(strPropertyValue).getTime()));
                } catch (ParseException pe) {
                    // try pasing date using https://github.com/joestelmach/natty
                    Parser parser = new Parser();
                    List<DateGroup> groups = parser.parse(strPropertyValue);
                    if(groups != null && groups.size() > 0) {
                        if (groups.get(0).getDates() != null && groups.get(0).getDates().size() > 0) {
                            long millis = groups.get(0).getDates().get(0).getTime();
                            return DateTimeValue.ofEpochMillis(Values.longValue(millis));
                        }
                    }

                    throw new BcException("Unrecognized date format value: " + rawPropertyValue, pe);
                }
            }
        }

        return Values.of(rawPropertyValue);
    }
}
