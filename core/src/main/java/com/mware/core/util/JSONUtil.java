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
package com.mware.core.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mware.core.model.clientapi.util.ObjectMapperFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.mware.core.exception.BcException;
import com.mware.core.exception.BcJsonParseException;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class JSONUtil {
    private static ObjectMapper mapper = ObjectMapperFactory.getInstance();

    public static JSONArray getOrCreateJSONArray(JSONObject json, String name) {
        JSONArray arr = json.optJSONArray(name);
        if (arr == null) {
            arr = new JSONArray();
            json.put(name, arr);
        }
        return arr;
    }

    public static boolean areEqual(Object o1, Object o2) throws JSONException {
        return fromJson(o1).equals(fromJson(o2));
    }

    public static void addToJSONArrayIfDoesNotExist(JSONArray jsonArray, Object value) {
        if (!arrayContains(jsonArray, value)) {
            jsonArray.put(value);
        }
    }

    public static boolean isInArray(JSONArray jsonArray, Object value) {
        return arrayIndexOf(jsonArray, value) >= 0;
    }

    public static int arrayIndexOf(JSONArray jsonArray, Object value) {
        for (int i = 0; i < jsonArray.length(); i++) {
            if (jsonArray.get(i).equals(value)) {
                return i;
            }
        }
        return -1;
    }

    public static boolean arrayContains(JSONArray jsonArray, Object value) {
        return arrayIndexOf(jsonArray, value) != -1;
    }

    public static void removeFromJSONArray(JSONArray jsonArray, Object value) {
        int idx = arrayIndexOf(jsonArray, value);
        if (idx >= 0) {
            jsonArray.remove(idx);
        }
    }

    public static Object parseObject(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            String valueString = (String) value;
            valueString = valueString.trim();
            if (valueString.startsWith("{") && valueString.endsWith("}")) {
                return new JSONObject(valueString);
            } else if (valueString.startsWith("[") && valueString.endsWith("]")) {
                return new JSONArray(valueString);
            } else {
                return value;
            }
        } else if (value instanceof JSONObject) {
            return value;
        } else if (value instanceof JSONArray) {
            return value;
        } else {
            throw new BcException("Could not parse object: " + value);
        }
    }

    public static JSONObject parse(String jsonString) {
        try {
            return new JSONObject(jsonString);
        } catch (JSONException ex) {
            throw new BcJsonParseException(jsonString, ex);
        }
    }

    public static JSONArray parseArray(String s) {
        try {
            return new JSONArray(s);
        } catch (JSONException ex) {
            throw new BcJsonParseException(s, ex);
        }
    }

    public static JsonNode toJsonNode(JSONObject json) {
        try {
            if (json == null) {
                return null;
            }
            return mapper.readTree(json.toString());
        } catch (IOException e) {
            throw new BcException("Could not create json node from: " + json.toString(), e);
        }
    }

    public static Map<String, String> toStringMap(JSONObject json) {
        Map<String, String> results = new HashMap<String, String>();
        for (Object key : json.keySet()) {
            String keyStr = (String) key;
            results.put(keyStr, json.getString(keyStr));
        }
        return results;
    }

    public static List<String> toStringList(JSONArray arr) {
        if (arr == null) {
            return null;
        }

        List<String> result = new ArrayList<String>();
        for (int i = 0; i < arr.length(); i++) {
            result.add(arr.getString(i));
        }
        return result;
    }

    public static Set<String> toStringSet(JSONArray arr) {
        if (arr == null) {
            return null;
        }
        return new HashSet<>(toStringList(arr));
    }

    public static List<Object> toList(JSONArray arr) {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < arr.length(); i++) {
            list.add(fromJson(arr.get(i)));
        }
        return list;
    }

    public static Map<String, Object> toMap(JSONObject obj) {
        Iterator<String> keys = obj.keys();
        Map<String, Object> map = new HashMap<>();
        while (keys.hasNext()) {
            String key = keys.next();
            map.put(key, fromJson(obj.get(key)));
        }
        return map;
    }

    public static JSONObject toJson(Map<String, ?> map) {
        JSONObject json = new JSONObject();
        for (Map.Entry<String, ?> e : map.entrySet()) {
            json.put(e.getKey(), toJson(e.getValue()));
        }
        return json;
    }

    public static Object toJson(Object value) {
        if (value instanceof Map) {
            return toJson((Map) value);
        } else if (value instanceof Iterable) {
            return toJson((Iterable) value);
        } else {
            return value;
        }
    }

    public static JSONArray toJson(Iterable iterable) {
        JSONArray json = new JSONArray();
        for (Object o : iterable) {
            json.put(toJson(o));
        }
        return json;
    }

    public static Long getOptionalLong(JSONObject json, String fieldName) {
        if (!json.has(fieldName) || json.isNull(fieldName)) {
            return null;
        }
        return json.getLong(fieldName);
    }

    private static Object fromJson(Object elem) throws JSONException {
        if (elem instanceof JSONObject) {
            return toMap((JSONObject) elem);
        } else if (elem instanceof JSONArray) {
            return toList((JSONArray) elem);
        } else {
            return elem;
        }
    }

    public static Stream<Object> stream(JSONArray jsonArray) {
        return toList(jsonArray).stream();
    }

    public static Stream<String> streamKeys(JSONObject data) {
        List<String> keys = new ArrayList<>();
        for (Object key : data.keySet()) {
            keys.add("" + key);
        }
        return keys.stream();
    }
}