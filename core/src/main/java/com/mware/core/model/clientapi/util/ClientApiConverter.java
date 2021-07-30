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
package com.mware.core.model.clientapi.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.ge.values.storable.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientApiConverter {
    public static Object toClientApiValue(Object value) {
        if (value instanceof JSONArray) {
            return toClientApiValue((JSONArray) value);
        } else if (value instanceof JSONObject) {
            return toClientApiValueInternal((JSONObject) value);
        } else if (JSONObject.NULL.equals(value)) {
            return null;
        } else if (value instanceof TextValue) {
            return toClientApiValue((TextValue) value);
        } else if (value instanceof DateTimeValue) {
            return toClientApiValue(((DateTimeValue) value).asObjectCopy().toInstant().toEpochMilli());
        } else if (value instanceof DurationValue) {
          return ((DurationValue)value).getAverageLengthInSeconds();
        } else if (value instanceof GeoCircleValue) {
          return ((GeoCircleValue)value).prettyPrint();
        } else if (value instanceof GeoRectValue) {
            return ((GeoRectValue)value).prettyPrint();
        } else if (value instanceof GeoLineValue) {
            return ((GeoLineValue) value).prettyPrint();
        } else if (value instanceof GeoPolygonValue) {
            return ((GeoPolygonValue)value).prettyPrint();
        } else if (value instanceof Value) {
            return ((Value) value).asObjectCopy();
        }
        return value;
    }

    private static List<Object> toClientApiValue(JSONArray json) {
        List<Object> result = new ArrayList<Object>();
        for (int i = 0; i < json.length(); i++) {
            Object obj = json.get(i);
            result.add(toClientApiValue(obj));
        }
        return result;
    }

    private static Object toClientApiValue(TextValue value) {
        String valueString = value.stringValue();
        try {
            valueString = valueString.trim();
            if (valueString.startsWith("{") && valueString.endsWith("}")) {
                return toClientApiValue((Object) new JSONObject(valueString));
            }
        } catch (Exception ex) {
            // ignore this exception it just mean the string wasn't really json
        }
        return valueString;
    }

    private static Object toClientApiValueInternal(JSONObject json) {
        if (json.length() == 2 && json.has("source") && json.has("workspaces")) {
            VisibilityJson visibilityJson = new VisibilityJson();
            visibilityJson.setSource(json.getString("source"));
            JSONArray workspacesJson = json.getJSONArray("workspaces");
            for (int i = 0; i < workspacesJson.length(); i++) {
                visibilityJson.addWorkspace(workspacesJson.getString(i));
            }
            return visibilityJson;
        }
        return toClientApiValue(json);
    }

    public static Map<String, Object> toClientApiValue(JSONObject json) {
        Map<String, Object> result = new HashMap<String, Object>();
        for (Object key : json.keySet()) {
            String keyStr = (String) key;
            result.put(keyStr, toClientApiValue(json.get(keyStr)));
        }
        return result;
    }

    public static Object fromClientApiValue(Object obj) {
        if (obj instanceof Map) {
            Map map = (Map) obj;
            if (VisibilityJson.isVisibilityJson(map)) {
                return VisibilityJson.fromMap(map);
            }
        }
        return obj;
    }

    public static String clientApiToString(Object o) {
        if (o == null) {
            throw new RuntimeException("o cannot be null.");
        }
        try {
            return ObjectMapperFactory.getInstance().writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not convert object '" + o.getClass().getName() + "' to string", e);
        }
    }

    public static <T> T toClientApi(JSONObject json, Class<T> clazz) {
        return toClientApi(json.toString(), clazz);
    }

    public static <T> T toClientApi(String str, Class<T> clazz) {
        try {
            return ObjectMapperFactory.getInstance().readValue(str, clazz);
        } catch (IOException e) {
            throw new RuntimeException("Could not parse '" + str + "' to class '" + clazz.getName() + "'", e);
        }
    }

    public static <T> Map<String, T> toClientApiMap(String str, Class<T> clazz) {
        try {
            MapType typeRef = TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, clazz);
            return ObjectMapperFactory.getInstance().readValue(str, typeRef);
        } catch (IOException e) {
            throw new RuntimeException("Could not parse '" + str + "' to class '" + clazz.getName() + "'", e);
        }
    }
}
