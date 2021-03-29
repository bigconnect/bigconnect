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
package com.mware.core.model.search;

import com.mware.core.exception.BcException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SearchOptions {
    private final Map<String, Object> parameters;
    private final String workspaceId;

    public SearchOptions(Map<String, Object> parameters, String workspaceId) {
        this.parameters = parameters;
        this.workspaceId = workspaceId;
    }

    public String getWorkspaceId() {
        return workspaceId;
    }

    public <T> T getOptionalParameter(String parameterName, Class<T> resultType) {
        Object obj = parameters.get(parameterName);
        if (obj == null) {
            return null;
        }
        try {
            if (resultType.isArray() && obj instanceof Collection) {
                Collection collection = (Collection) obj;
                Class type = resultType.getComponentType();
                return (T) collection.toArray((Object[]) Array.newInstance(type, collection.size()));
            } else if (resultType.isArray() && !obj.getClass().isArray()) {
                Object[] array = (Object[]) Array.newInstance(resultType.getComponentType(), 1);
                array[0] = objectToType(obj, resultType.getComponentType());
                return objectToType(array, resultType);
            }
            return objectToType(obj, resultType);
        } catch (Exception ex) {
            throw new BcException("Could not cast object \"" + obj + "\" to type \"" + resultType.getName() + "\"", ex);
        }
    }

    private <T> T objectToType(Object obj, Class<T> resultType) {
        if (obj != null && resultType == obj.getClass()) {
            //noinspection unchecked
            return (T) obj;
        }
        if (resultType == Integer.class && obj instanceof String) {
            return resultType.cast(Integer.parseInt((String) obj));
        }
        if (resultType == Long.class && obj instanceof String) {
            return resultType.cast(Long.parseLong((String) obj));
        }
        if (resultType == Long.class && obj instanceof Integer) {
            return resultType.cast(((Integer) obj).longValue());
        }
        if (resultType == Double.class && obj instanceof String) {
            return resultType.cast(Double.parseDouble((String) obj));
        }
        if (resultType == Float.class && obj instanceof String) {
            return resultType.cast(Float.parseFloat((String) obj));
        }
        if (resultType == JSONArray.class && obj instanceof String) {
            return resultType.cast(new JSONArray((String) obj));
        }
        if (resultType == JSONArray.class && obj instanceof String[]) {
            return resultType.cast(new JSONArray(obj));
        }
        if (resultType == Boolean.class && obj instanceof String) {
            return resultType.cast(Boolean.parseBoolean((String) obj));
        }
        if (resultType == String.class && obj instanceof JSONObject) {
            return resultType.cast(obj.toString());
        }
        return resultType.cast(obj);
    }

    public <T> T getOptionalParameter(String parameterName, T defaultValue) {
        checkNotNull(defaultValue, "defaultValue cannot be null");
        T obj = (T) getOptionalParameter(parameterName, defaultValue.getClass());
        if (obj == null) {
            // null is a possible value, for example limit=null signifies don't limit the results. If limit is
            // not specified use the defaultValue
            if (parameters.containsKey(parameterName)) {
                return null;
            }
            return defaultValue;
        }
        return obj;
    }

    public <T> T getRequiredParameter(String parameterName, Class<T> resultType) {
        T obj = getOptionalParameter(parameterName, resultType);
        if (obj == null) {
            throw new BcException("Missing parameter: " + parameterName);
        }
        return obj;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return "SearchOptions{" +
                "parameters=" + parameters +
                ", workspaceId='" + workspaceId + '\'' +
                '}';
    }
}
