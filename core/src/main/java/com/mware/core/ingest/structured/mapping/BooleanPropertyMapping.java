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
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BooleanPropertyMapping extends PropertyMapping {
    public static final String PROPERTY_MAPPING_BOOLEAN_TRUE_KEY = "trueValues";
    public static final String PROPERTY_MAPPING_BOOLEAN_FALSE_KEY = "falseValues";
    public static final String PROPERTY_MAPPING_DEFAULT_KEY = "defaultValue";

    public List<String> trueValues = new ArrayList<>();
    public List<String> falseValues = new ArrayList<>();

    private Boolean defaultValue;

    public BooleanPropertyMapping() {
    }

    public BooleanPropertyMapping(
            VisibilityTranslator visibilityTranslator, String workspaceId, JSONObject propertyMapping) {
        super(visibilityTranslator, workspaceId, propertyMapping);

        String defaultValueStr = propertyMapping.optString(PROPERTY_MAPPING_DEFAULT_KEY);
        if(!StringUtils.isBlank(defaultValueStr)) {
            defaultValue = Boolean.parseBoolean(defaultValueStr);
        }

        JSONArray trueValueMappings = propertyMapping.getJSONArray(PROPERTY_MAPPING_BOOLEAN_TRUE_KEY);
        JSONArray falseValueMappings = propertyMapping.getJSONArray(PROPERTY_MAPPING_BOOLEAN_FALSE_KEY);

        for (int i = 0; i < trueValueMappings.length(); i++) {
            trueValues.add(trueValueMappings.getString(i).toLowerCase());
        }
        for (int i = 0; i < falseValueMappings.length(); i++) {
            falseValues.add(falseValueMappings.getString(i).toLowerCase());
        }
    }

    public BooleanPropertyMapping(VisibilityTranslator visibilityTranslator, String workspaceId, ClientApiDataSource.EntityMapping mapping) {
        super(visibilityTranslator, workspaceId, mapping);
        trueValues.addAll(Arrays.asList(mapping.getColPropTrueValues().split(",")));
        falseValues.addAll(Arrays.asList(mapping.getColPropFalseValues().split(",")));
    }

    @Override
    public Value decodeValue(Object rawPropertyValue) {
        Boolean result = defaultValue;
        if (rawPropertyValue instanceof String && !StringUtils.isBlank((String)rawPropertyValue)) {
            String value = (String) rawPropertyValue;
            rawPropertyValue = value.toLowerCase();
            if (trueValues.contains(rawPropertyValue)) {
                result = Boolean.TRUE;
            } else if (falseValues.contains(rawPropertyValue)) {
                result = Boolean.FALSE;
            } else {
                Boolean bool = BooleanUtils.toBooleanObject(value);
                if (bool != null) {
                    return Values.booleanValue(result);
                }

                throw new BcException("Unrecognized boolean value: " + rawPropertyValue);
            }
        }

        return Values.booleanValue(result);
    }
}
