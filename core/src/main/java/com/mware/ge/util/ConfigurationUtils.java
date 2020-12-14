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
package com.mware.ge.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import com.mware.ge.Graph;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.GeException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mware.ge.util.Preconditions.checkNotNull;

public class ConfigurationUtils {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(ConfigurationUtils.class);

    public static <T> T createProvider(Graph graph, GraphConfiguration config, String propPrefix, String defaultProvider) throws GeException {
        String implClass = config.getString(propPrefix, defaultProvider);
        checkNotNull(implClass, "createProvider could not find " + propPrefix + " configuration item");
        return createProvider(implClass, graph, config);
    }

    @SuppressWarnings("unchecked")
    public static <T> T createProvider(String className, Graph graph, GraphConfiguration config) throws GeException {
        checkNotNull(className, "className is required");
        className = className.trim();
        LOGGER.debug("creating provider '%s'", className);
        Class<Graph> graphClass = Graph.class;
        Class<GraphConfiguration> graphConfigurationClass = GraphConfiguration.class;
        try {
            Class<?> clazz = Class.forName(className);
            try {
                Constructor constructor;
                try {
                    constructor = clazz.getConstructor(graphClass);
                    return (T) constructor.newInstance(graph);
                } catch (NoSuchMethodException ignore1) {
                    try {
                        constructor = clazz.getConstructor(graphClass, graphConfigurationClass);
                        return (T) constructor.newInstance(graph, config);
                    } catch (NoSuchMethodException ignore2) {
                        try {
                            constructor = clazz.getConstructor(graphConfigurationClass);
                            return (T) constructor.newInstance(config);
                        } catch (NoSuchMethodException ignore3) {
                            try {
                                constructor = clazz.getConstructor(Map.class);
                                return (T) constructor.newInstance(config.getConfig());
                            } catch (NoSuchMethodException ignoreInner) {
                                constructor = clazz.getConstructor();
                                return (T) constructor.newInstance();
                            }
                        }
                    }
                }
            } catch (IllegalArgumentException e) {
                StringBuilder possibleMatches = new StringBuilder();
                for (Constructor<?> s : clazz.getConstructors()) {
                    possibleMatches.append(s.toGenericString());
                    possibleMatches.append(", ");
                }
                throw new GeException("Invalid constructor for " + className + ". Expected <init>(" + graphConfigurationClass.getName() + "). Found: " + possibleMatches, e);
            }
        } catch (NoSuchMethodException e) {
            throw new GeException("Provider must have a single argument constructor taking a " + graphConfigurationClass.getName(), e);
        } catch (ClassNotFoundException e) {
            throw new GeException("No provider found with class name " + className, e);
        } catch (Exception e) {
            throw new GeException(e);
        }
    }

    public static Map<String, String> loadConfig(List<String> configFileNames, String configPropertyPrefix) throws IOException {
        Map<String, String> props = loadFiles(configFileNames);
        props.putAll(System.getenv());
        resolvePropertyReferences(props);
        return stripPrefix(props, configPropertyPrefix);
    }

    private static Map<String, String> loadFiles(List<String> configFileNames) throws IOException {
        Properties props = new Properties();
        for (String configFileName : configFileNames) {
            File configFile = new File(configFileName);
            if (!configFile.exists()) {
                throw new RuntimeException("Could not load config file: " + configFile.getAbsolutePath());
            }

            try (InputStream in = new FileInputStream(configFile)) {
                props.load(in);
            }
        }
        return propertiesToMap(props);
    }

    public static Map<String, String> stripPrefix(Map<String, String> propsMap, String configPropertyPrefix) {
        Map<String, String> result = new HashMap<>();
        if (configPropertyPrefix == null) {
            result.putAll(propsMap);
        } else {
            for (Map.Entry<String, String> p : propsMap.entrySet()) {
                if (p.getKey().startsWith(configPropertyPrefix + ".")) {
                    result.put(p.getKey().substring((configPropertyPrefix + ".").length()), p.getValue());
                } else if (p.getKey().startsWith(configPropertyPrefix)) {
                    result.put(p.getKey().substring(configPropertyPrefix.length()), p.getValue());
                }
            }
        }

        return result;
    }

    private static Map<String, String> propertiesToMap(Properties props) {
        Map<String, String> results = new HashMap<>();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            results.put("" + entry.getKey(), "" + entry.getValue());
        }
        return results;
    }

    private static void resolvePropertyReferences(Map<String, String> config) {
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String entryValue = (String) ((Map.Entry) entry).getValue();
            if (!StringUtils.isBlank(entryValue)) {
                entry.setValue(StrSubstitutor.replace(entryValue, config));
            }
        }
    }

    public static String getString(Map<String, Object> config, String configKey, String defaultValue) {
        Object str = config.get(configKey);
        if (str == null) {
            return defaultValue;
        }
        if (str instanceof String) {
            return ((String) str).trim();
        }
        return str.toString().trim();
    }

    public static long getConfigLong(Map<String, Object> config, String key, long defaultValue) {
        Object obj = config.get(key);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        if (obj instanceof Long) {
            return (long) obj;
        }
        return Long.parseLong(obj.toString());
    }

    public static Duration getDuration(Map<String, Object> config, String key, Duration defaultValue) {
        return getDuration(config, key, defaultValue.toMillis() + "ms");
    }

    public static Duration getDuration(Map<String, Object> config, String key, String defaultValue) {
        String value = getString(config, key, defaultValue);
        try {
            return Duration.parse(value);
        } catch (DateTimeParseException ex) {
            Matcher m = Pattern.compile("(\\d+)(\\D+)").matcher(value);
            if (!m.matches()) {
                throw ex;
            }
            long digits = Long.parseLong(m.group(1));
            String units = m.group(2);
            switch (units) {
                case "ms":
                    return Duration.ofMillis(digits);
                case "s":
                    return Duration.ofSeconds(digits);
                case "m":
                    return Duration.ofMinutes(digits);
                case "h":
                    return Duration.ofHours(digits);
                default:
                    throw new GeException("unhandled duration units: " + value);
            }
        }
    }

    public static Integer getInteger(Map<String, Object> config, String configKey, Integer defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        if (obj instanceof Integer) {
            return (int) obj;
        }
        return Integer.valueOf(obj.toString());
    }

    public static int getInt(Map<String, Object> config, String configKey, int defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        if (obj instanceof Integer) {
            return (int) obj;
        }
        return Integer.parseInt(obj.toString());
    }

    public static double getDouble(Map<String, Object> config, String configKey, double defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        }
        if (obj instanceof Double) {
            return (double) obj;
        }
        return Double.parseDouble(obj.toString());
    }

    public static boolean getBoolean(Map<String, Object> config, String configKey, boolean defaultValue) {
        Object obj = config.get(configKey);
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof String) {
            return Boolean.parseBoolean((String) obj);
        }
        if (obj instanceof Boolean) {
            return (boolean) obj;
        }
        return Boolean.parseBoolean(obj.toString());
    }
}
