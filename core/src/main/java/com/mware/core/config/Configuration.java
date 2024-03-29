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
package com.mware.core.config;

import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.Relationship;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.user.PrivilegeRepository;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.json.JSONObject;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.reflections.ReflectionUtils.*;

/**
 * Responsible for parsing application configuration file and providing
 * configuration values to the application
 */
public class Configuration extends TypedConfiguration {
    public static final String WEB_CONFIGURATION_PREFIX = "web.ui.";

    public static final String DW_QUEUE_PREFIX = "dw.queue";
    public static final String DW_QUEUE_NAME = "dw.queue.name";
    public static final String LRP_QUEUE_PREFIX = "lrp.queue";
    public static final String LRP_QUEUE_NAME = "lrp.queue.name";

    public static final String SYSTEM_PROPERTY_PREFIX = "bc.";

    private final ConfigurationLoader configurationLoader;
    private PrivilegeRepository privilegeRepository;
    private SchemaRepository schemaRepository;

    static {
        // register all options
        registerOptionHolders("com.mware");
        registerOptionHolders("io.bigconnect");
    }

    private static void registerOptionHolders(String packageName) {
        Set<Class<? extends OptionHolder>> optionHolders = new Reflections(packageName, new SubTypesScanner())
                .getSubTypesOf(OptionHolder.class);
        optionHolders.forEach(optionHolder -> {
            Set<Method> initMethod = getMethods(optionHolder, withModifier(Modifier.STATIC), withReturnType(optionHolder), withParameters());
            if (!initMethod.isEmpty()) {
                Method m = initMethod.iterator().next();
                try {
                    OptionHolder instance = (OptionHolder) m.invoke(null);
                    OptionSpace.register(optionHolder.getSimpleName(), instance);
                } catch (Exception e) {
                    // do nothing
                }
            } else {
                LOGGER.warn("Skipping config option holder %s because it's missing a static instance() method.", optionHolder.getName());
            }
        });
    }

    public static final Configuration EMPTY = new Configuration();

    private Configuration() {
        super(new ConcurrentHashMap<>());
        this.configurationLoader = new HashMapConfigurationLoader("");
    }

    public Configuration(final Map<String, Object> config) {
        this(null, config);
    }

    public Configuration(final ConfigurationLoader configurationLoader, final Map<String, Object> config) {
        super(new ConcurrentHashMap<>());
        this.configurationLoader = configurationLoader;
        // set default values
        OptionSpace.keys().forEach(k -> set(k, OptionSpace.get(k).defaultValue()));
        addConfigMapEntries(config);
        addSystemProperties();
        resolvePropertyReferences();
    }

    private void addConfigMapEntries(Map<String, ?> config) {
        for (Map.Entry<String, ?> entry : config.entrySet()) {
            if (entry.getValue() != null) {
                set(entry.getKey(), entry.getValue());
            }
        }
    }

    private void addSystemProperties() {
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            String key = entry.getKey().toString();
            if (key.startsWith(SYSTEM_PROPERTY_PREFIX)) {
                key = key.substring(SYSTEM_PROPERTY_PREFIX.length());
                Object value = entry.getValue();
                set(key, value);
            }
        }
    }

    private void resolvePropertyReferences() {
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            Object entryValue = entry.getValue();
            if (entryValue instanceof String && !StringUtils.isBlank((String) entryValue)) {
                entry.setValue(StrSubstitutor.replace(entryValue, System.getenv()));
            }
        }
    }

    public <T> T setConfigurables(T o, String keyPrefix) {
        Map<String, Object> subset = getSubset(keyPrefix);
        return setConfigurables(o, subset);
    }

    public static <T> T setConfigurables(T o, Map<String, Object> config) {
        ConvertUtilsBean convertUtilsBean = new ConvertUtilsBean();
        Map<Method, PostConfigurationValidator> validatorMap = new HashMap<>();

        for (Method m : o.getClass().getMethods()) {
            setConfigurablesMethod(o, m, config, convertUtilsBean);

            PostConfigurationValidator validatorAnnotation = m.getAnnotation(PostConfigurationValidator.class);
            if (validatorAnnotation != null) {
                if (m.getParameterTypes().length != 0) {
                    throw new BcException("Invalid validator method " + o.getClass().getName() + "." + m.getName() + "(). Expected 0 arguments. Found " + m.getParameterTypes().length + " arguments");
                }
                if (m.getReturnType() != Boolean.TYPE) {
                    throw new BcException("Invalid validator method " + o.getClass().getName() + "." + m.getName() + "(). Expected Boolean return type. Found " + m.getReturnType());
                }
                validatorMap.put(m, validatorAnnotation);
            }
        }

        List<Field> fields = getAllFields(o);
        for (Field f : fields) {
            setConfigurablesField(o, f, config, convertUtilsBean);
        }

        for (Method postConfigurationValidatorMethod : validatorMap.keySet()) {
            try {
                if (!(Boolean) postConfigurationValidatorMethod.invoke(o)) {
                    String description = validatorMap.get(postConfigurationValidatorMethod).description();
                    description = description.equals("") ? "()" : "(" + description + ")";
                    throw new BcException(o.getClass().getName() + "." + postConfigurationValidatorMethod.getName() + description + " returned false");
                }
            } catch (InvocationTargetException e) {
                throw new BcException("InvocationTargetException invoking validator " + o.getClass().getName() + "." + postConfigurationValidatorMethod.getName(), e);
            } catch (IllegalAccessException e) {
                throw new BcException("IllegalAccessException invoking validator " + o.getClass().getName() + "." + postConfigurationValidatorMethod.getName(), e);
            }
        }

        return o;
    }

    private static List<Field> getAllFields(Object o) {
        List<Field> fields = new ArrayList<>();
        Class<?> c = o.getClass();
        while (c != null) {
            Collections.addAll(fields, c.getDeclaredFields());
            c = c.getSuperclass();
        }
        return fields;
    }

    private static void setConfigurablesMethod(Object o, Method m, Map<String, Object> config, ConvertUtilsBean convertUtilsBean) {
        Configurable configurableAnnotation = m.getAnnotation(Configurable.class);
        if (configurableAnnotation == null) {
            return;
        }
        if (m.getParameterTypes().length != 1) {
            throw new BcException("Invalid method to be configurable. Expected 1 argument. Found " + m.getParameterTypes().length + " arguments");
        }

        String propName = m.getName().substring("set".length());
        if (propName.length() > 1) {
            propName = propName.substring(0, 1).toLowerCase() + propName.substring(1);
        }

        setConfigurable(config, configurableAnnotation, propName, m.getParameterTypes()[0], false, convertUtilsBean, value -> {
            try {
                m.invoke(o, value);
            } catch (Exception ex) {
                throw new BcException("Could not set property " + m.getName() + " on " + o.getClass().getName());
            }
        });
    }

    private static void setConfigurablesField(Object o, Field f, Map<String, Object> config, ConvertUtilsBean convertUtilsBean) {
        Configurable configurableAnnotation = f.getAnnotation(Configurable.class);
        if (configurableAnnotation == null) {
            return;
        }

        String propName = f.getName();

        setConfigurable(config, configurableAnnotation, propName, f.getType(), true, convertUtilsBean, value -> {
            try {
                f.setAccessible(true);
                f.set(o, value);
            } catch (Exception ex) {
                throw new BcException("Could not set property " + f.getName() + " on " + o.getClass().getName());
            }
        });
    }

    private static void setConfigurable(
            Map<String, Object> config,
            Configurable configurableAnnotation,
            String propName,
            Class<?> propType,
            boolean isField,
            ConvertUtilsBean convertUtilsBean,
            Consumer<Object> setFunction
    ) {
        String name = configurableAnnotation.name();
        if (name.equals(Configurable.DEFAULT_NAME)) {
            name = propName;
        }

        if (Map.class.isAssignableFrom(propType)) {
            SortedMap<String, Map<String, Object>> values = getMultiValue(config.entrySet(), name);
            setFunction.accept(values);
            return;
        }

        Object val;
        if (config.containsKey(name)) {
            val = config.get(name);
        } else {
            if (Configurable.DEFAULT_VALUE.equals(configurableAnnotation.defaultValue())) {
                if (isField) {
                    return; // fields always have a default value, we should use that value
                }
                if (configurableAnnotation.required()) {
                    throw new BcException(String.format("Could not find property \"%s\" and no default value was specified.", name));
                } else {
                    return;
                }
            }
            val = configurableAnnotation.defaultValue();
        }
        Object convertedValue = convertUtilsBean.convert(val, propType);
        setFunction.accept(convertedValue);
    }

    public JSONObject toJSON(String workspaceId, ResourceBundle resourceBundle) {
        JSONObject properties = new JSONObject();

        SchemaRepository schemaRepository = InjectHelper.getInstance(SchemaRepository.class);
        for (Concept concept : schemaRepository.getConceptsWithProperties(workspaceId)) {
            for (String intent : concept.getIntents()) {
                properties.put(SchemaRepository.CONFIG_INTENT_CONCEPT_PREFIX + intent, concept.getName());
            }
        }
        for (SchemaProperty property : schemaRepository.getProperties(workspaceId)) {
            for (String intent : property.getIntents()) {
                properties.put(SchemaRepository.CONFIG_INTENT_PROPERTY_PREFIX + intent, property.getName());
            }
        }
        for (Relationship relationship : schemaRepository.getRelationships(workspaceId)) {
            for (String intent : relationship.getIntents()) {
                properties.put(SchemaRepository.CONFIG_INTENT_RELATIONSHIP_PREFIX + intent, relationship.getName());
            }
        }

        for (String key : getKeys()) {
            if (key.startsWith(Configuration.WEB_CONFIGURATION_PREFIX)) {
                properties.put(key.replaceFirst(Configuration.WEB_CONFIGURATION_PREFIX, ""), get(key, ""));
            } else if (key.startsWith("ontology.intent")) {
                properties.put(key, get(key, ""));
            }
        }

        PrivilegeRepository privilegeRepository = getPrivilegeRepository();
        Set<String> allPrivileges = privilegeRepository.getAllPrivileges().stream()
                .map(Privilege::getName)
                .collect(Collectors.toSet());
        properties.put("privileges", Privilege.toJson(allPrivileges));

        JSONObject messages = new JSONObject();
        if (resourceBundle != null) {
            for (String key : resourceBundle.keySet()) {
                messages.put(key, resourceBundle.getString(key));
            }
        }

        JSONObject configuration = new JSONObject();
        configuration.put("properties", properties);
        configuration.put("messages", messages);

        return configuration;
    }

    public JSONObject getJsonProperties() {
        JSONObject properties = new JSONObject();
        for (String key : config.keySet()) {
            if (key.toLowerCase().contains("password")) {
                properties.put(key, "********");
            } else {
                properties.put(key, config.get(key));
            }
        }
        return properties;
    }

    /**
     * Similar to {@link Configuration#getMultiValue(java.lang.String)}, but returns a new instance of
     * a configurable type for each prefix.
     * <p>
     * Given the following configuration:
     * <p>
     * <pre><code>
     * item.config.set1.key1=value1
     * item.config.set1.key2=value2
     *
     * item.config.set2.key1=value3
     * item.config.set2.key2=value4
     * </pre></code>
     * <p>
     * And the following class.
     *
     * <pre><code>
     * class ItemConfig {
     *   {@literal @}Configurable
     *   public String key1;
     *
     *   {@literal @}Configurable
     *   public String key2;
     * }
     * </core></pre>
     * <p>
     * Would produce a map with two keys "set1" and "set2" mapped to an ItemConfig object.
     *
     * @param prefix           The configuration key prefix
     * @param configurableType The type of each configurable object to create instances of
     */
    public <T> Map<String, T> getMultiValueConfigurables(String prefix, Class<T> configurableType) {
        Map<String, Map<String, Object>> multiValues = getMultiValue(prefix);
        Map<String, T> results = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : multiValues.entrySet()) {
            T o;
            try {
                o = configurableType.newInstance();
            } catch (Exception e) {
                throw new BcException("Could not create configurable: " + configurableType.getName(), e);
            }
            setConfigurables(o, entry.getValue());
            results.put(entry.getKey(), o);
        }
        return results;
    }

    /**
     * Similar to {@link Configuration#getMultiValue(java.lang.Iterable, java.lang.String)} but uses the internal
     * configuration state.
     */
    public Map<String, Map<String, Object>> getMultiValue(String prefix) {
        return getMultiValue(this.config.entrySet(), prefix);
    }

    /**
     * Processing configuration items that looks like this:
     * <p></p>
     * <pre><code>
     * item.config.set1.key1=value1
     * item.config.set1.key2=value2
     *
     * item.config.set2.key1=value3
     * item.config.set2.key2=value4
     * </code></pre>
     * <p></p>
     * Into a hash like this:
     * <p></p>
     * - set1
     * - key1: value1
     * - key2: value2
     * - set2
     * - key1: value3
     * - key2: value4
     */
    public static SortedMap<String, Map<String, Object>> getMultiValue(Iterable<Map.Entry<String, Object>> config, String prefix) {
        if (!prefix.endsWith(".")) {
            prefix = prefix + ".";
        }

        SortedMap<String, Map<String, Object>> results = new TreeMap<>();
        for (Map.Entry<String, Object> item : config) {
            if (item.getKey().startsWith(prefix)) {
                String rest = item.getKey().substring(prefix.length());
                int ch = rest.indexOf('.');
                String key;
                String subkey;
                if (ch > 0) {
                    key = rest.substring(0, ch);
                    subkey = rest.substring(ch + 1);
                } else {
                    key = rest;
                    subkey = "";
                }
                Map<String, Object> values = results.computeIfAbsent(key, k -> new HashMap<>());
                values.put(subkey, item.getValue());
            }
        }
        return results;
    }

    public JSONObject getConfigurationInfo() {
        if (configurationLoader != null)
            return configurationLoader.getConfigurationInfo();
        else
            return new JSONObject();
    }

    protected SchemaRepository getSchemaRepository() {
        if (schemaRepository == null) {
            schemaRepository = InjectHelper.getInstance(SchemaRepository.class);
        }
        return schemaRepository;
    }

    protected PrivilegeRepository getPrivilegeRepository() {
        if (privilegeRepository == null) {
            privilegeRepository = InjectHelper.getInstance(PrivilegeRepository.class);
        }
        return privilegeRepository;
    }
}
