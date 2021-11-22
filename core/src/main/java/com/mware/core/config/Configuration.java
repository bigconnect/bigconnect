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
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ClassUtil;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.json.JSONObject;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Responsible for parsing application configuration file and providing
 * configuration values to the application
 */
public class Configuration {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(Configuration.class);
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static final String BASE_URL = "base.url";

    public static final String LOCK_REPOSITORY = "repository.lock";
    public static final String TRACE_REPOSITORY = "repository.trace";
    public static final String TRACE_ENABLED = "trace.enabled";
    public static final String USER_REPOSITORY = "repository.user";
    public static final String WORKSPACE_REPOSITORY = "repository.workspace";
    public static final String GRAPH_AUTHORIZATION_REPOSITORY = "repository.graphAuthorization";
    public static final String ONTOLOGY_REPOSITORY = "repository.ontology";
    public static final String USER_SESSION_COUNTER_REPOSITORY = "repository.userSessionCounter";
    public static final String WORK_QUEUE_REPOSITORY = "repository.workQueue";
    public static final String WEB_QUEUE_REPOSITORY = "repository.webQueue";
    public static final String LONG_RUNNING_PROCESS_REPOSITORY = "repository.longRunningProcess";
    public static final String REGEX_REPOSITORY = "repository.regex";
    public static final String SIMPLE_ORM_SESSION = "simpleOrmSession";
    public static final String EMAIL_REPOSITORY = "repository.email";
    public static final String STATUS_REPOSITORY = "repository.status";
    public static final String ACL_PROVIDER_REPOSITORY = "repository.acl";
    public static final String FILE_SYSTEM_REPOSITORY = "repository.fileSystem";
    public static final String AUTHORIZATION_REPOSITORY = "repository.authorization";
    public static final String PRIVILEGE_REPOSITORY = "repository.privilege";
    public static final String CACHE_SERVICE = "service.cache";
    public static final String AUDIT_SERVICE = "service.audit";
    public static final String GRAPH_PROVIDER = "graph";
    public static final String VISIBILITY_TRANSLATOR = "security.visibilityTranslator";
    public static final String WEB_CONFIGURATION_PREFIX = "web.ui.";
    public static final String WEB_GEOCODER_ENABLED = WEB_CONFIGURATION_PREFIX + "geocoder.enabled";
    public static final String DEV_MODE = "devMode";
    public static final boolean DEV_MODE_DEFAULT = true;
    public static final String DEFAULT_SEARCH_RESULT_COUNT = "search.defaultSearchCount";
    public static final String DEFAULT_TIME_ZONE = "default.timeZone";
    public static final String RABBITMQ_PREFETCH_COUNT = "rabbitmq.prefetch.count";

    public static final String DW_QUEUE_PREFIX = "dw.queue";
    public static final String DW_QUEUE_NAME = "dw.queue.name";
    public static final String LRP_QUEUE_PREFIX = "lrp.queue";
    public static final String LRP_QUEUE_NAME = "lrp.queue.name";

    public static final String STATUS_PORT_RANGE = "status.portRange";
    public static final String DEFAULT_STATUS_PORT_RANGE = "40000-41000";
    public static final String COMMENTS_AUTO_PUBLISH = "comments.autoPublish";
    public static final boolean DEFAULT_COMMENTS_AUTO_PUBLISH = false;
    public static final String MULTIPART_LOCATION = "multipart.location";
    public static final String DEFAULT_MULTIPART_LOCATION = System.getProperty("java.io.tmpdir");
    public static final String MULTIPART_MAX_FILE_SIZE = "multipart.maxFileSize";
    public static final long DEFAULT_MULTIPART_MAX_FILE_SIZE = 1024 * 1024 * 512;
    public static final String MULTIPART_MAX_REQUEST_SIZE = "multipart.maxRequestSize";
    public static final long DEFAULT_MULTIPART_MAX_REQUEST_SIZE = 1024 * 1024 * 1024;
    public static final String MULTIPART_FILE_SIZE_THRESHOLD = "multiPart.fileSizeThreshold";
    public static final int DEFAULT_MULTIPART_FILE_SIZE_THRESHOLD = 0;
    public static final boolean DEFAULT_TRACE_ENABLED = false;
    public static final String STATUS_REFRESH_INTERVAL_SECONDS = "status.refreshIntervalSeconds";
    public static final int STATUS_REFRESH_INTERVAL_SECONDS_DEFAULT = 10;
    public static final String SYSTEM_PROPERTY_PREFIX = "bc.";
    public static final String AUTH_TOKEN_PASSWORD = "auth.token.password";
    public static final String AUTH_TOKEN_SALT = "auth.token.salt";
    public static final String AUTH_TOKEN_EXPIRATION_IN_MINS = WEB_CONFIGURATION_PREFIX + "auth.token.expiration_minutes";
    public static final String AUTH_TOKEN_EXPIRATION_TOLERANCE_IN_SECS = "auth.token.expiration_tolerance_seconds";
    public static final String HTTP_GZIP_ENABLED = "http.gzipEnabled";
    public static final String WEB_RESPONSE_HEADER_X_FRAME_OPTIONS = "web.response.header.X-Frame-Options";

    private final ConfigurationLoader configurationLoader;
    private PrivilegeRepository privilegeRepository;
    private SchemaRepository schemaRepository;

    private Map<String, String> config = new HashMap<>();

    public static final Map<String, String> DEFAULTS = new HashMap<>();
    static {
        DEFAULTS.put(AUTH_TOKEN_EXPIRATION_IN_MINS, "60");
        DEFAULTS.put(AUTH_TOKEN_EXPIRATION_TOLERANCE_IN_SECS, "60");
        DEFAULTS.put(WEB_RESPONSE_HEADER_X_FRAME_OPTIONS, "DENY");
    }

    public static final Configuration EMPTY = new Configuration();

    private Configuration() {
        this.configurationLoader = new HashMapConfigurationLoader("");
    }

    public Configuration(final ConfigurationLoader configurationLoader, final Map<?, ?> config) {
        this.configurationLoader = configurationLoader;
        addConfigMapEntries(config);
        addSystemProperties();
        resolvePropertyReferences();
    }

    private void addConfigMapEntries(Map<?, ?> config) {
        for (Map.Entry entry : config.entrySet()) {
            if (entry.getValue() != null) {
                set(entry.getKey().toString(), entry.getValue());
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
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String entryValue = entry.getValue();
            if (!StringUtils.isBlank(entryValue)) {
                entry.setValue(StrSubstitutor.replace(entryValue, config));
            }
        }
    }

    public String get(String propertyKey, String defaultValue) {
        return config.containsKey(propertyKey) ? config.get(propertyKey) : defaultValue;
    }

    public boolean getBoolean(String propertyKey, boolean defaultValue) {
        return Boolean.parseBoolean(get(propertyKey, Boolean.toString(defaultValue)));
    }

    public Integer getInt(String propertyKey, Integer defaultValue) {
        return Integer.parseInt(get(propertyKey, defaultValue == null ? null : defaultValue.toString()));
    }

    public Integer getInt(String propertyKey) {
        return getInt(propertyKey, null);
    }

    public Long getLong(String propertyKey, Long defaultValue) {
        return Long.parseLong(get(propertyKey, defaultValue == null ? null : defaultValue.toString()));
    }

    public Long getLong(String propertyKey) {
        return getLong(propertyKey, null);
    }

    public <T> Class<? extends T> getClass(String propertyKey) {
        return getClass(propertyKey, null);
    }

    public <T> Class<? extends T> getClass(String propertyKey, Class<? extends T> defaultClass) {
        String className = get(propertyKey, defaultClass == null ? null : defaultClass.getName());
        if (className == null) {
            throw new BcException("Could not find required property " + propertyKey);
        }
        className = className.trim();
        try {
            LOGGER.debug("found class \"%s\" for configuration \"%s\"", className, propertyKey);
            return ClassUtil.forName(className);
        } catch (BcException e) {
            throw new BcException("Could not load class " + className + " for property " + propertyKey, e);
        }
    }

    public Map<String, String> getSubset(String keyPrefix) {
        Map<String, String> subset = new HashMap<>();
        for (Map.Entry<String, String> entry : this.config.entrySet()) {
            if (!entry.getKey().startsWith(keyPrefix + ".") && !entry.getKey().equals(keyPrefix)) {
                continue;
            }
            String newKey = entry.getKey().substring(keyPrefix.length());
            if (newKey.startsWith(".")) {
                newKey = newKey.substring(1);
            }
            subset.put(newKey, entry.getValue());
        }
        return subset;
    }

    public <T> T setConfigurables(T o, String keyPrefix) {
        Map<String, String> subset = getSubset(keyPrefix);
        return setConfigurables(o, subset);
    }

    public static <T> T setConfigurables(T o, Map<String, String> config) {
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
        Class c = o.getClass();
        while (c != null) {
            Collections.addAll(fields, c.getDeclaredFields());
            c = c.getSuperclass();
        }
        return fields;
    }

    private static void setConfigurablesMethod(Object o, Method m, Map<String, String> config, ConvertUtilsBean convertUtilsBean) {
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

    private static void setConfigurablesField(Object o, Field f, Map<String, String> config, ConvertUtilsBean convertUtilsBean) {
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
            Map<String, String> config,
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
            SortedMap<String, Map<String, String>> values = getMultiValue(config.entrySet(), name);
            setFunction.accept(values);
            return;
        }

        String val;
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

    public Map toMap() {
        return this.config;
    }

    public Iterable<String> getKeys() {
        return this.config.keySet();
    }

    public Iterable<String> getKeys(String keyPrefix) {
        getSubset(keyPrefix).keySet();
        Set<String> keys = new TreeSet<>();
        for (String key : getKeys()) {
            if (key.startsWith(keyPrefix)) {
                keys.add(key);
            }
        }
        return keys;
    }

    public void set(String propertyKey, Object value) {
        if (value == null) {
            config.remove(propertyKey);
        } else {
            config.put(propertyKey, value.toString().trim());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        SortedSet<String> keys = new TreeSet<>(this.config.keySet());

        boolean first = true;
        for (String key : keys) {
            if (first) {
                first = false;
            } else {
                sb.append(LINE_SEPARATOR);
            }
            if (key.toLowerCase().contains("password")) {
                sb.append(key).append(": ********");
            } else {
                sb.append(key).append(": ").append(get(key, null));
            }
        }

        return sb.toString();
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
            } else if (key.startsWith("ontology.intent") || key.startsWith("watcher.enabled") || key.startsWith("ldap.enabled")) {
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
     * repository.ontology.owl.dev.iri=http://bigconnect/dev
     * repository.ontology.owl.dev.dir=examples/ontology-dev/
     *
     * repository.ontology.owl.csv.iri=http://bigconnect/csv
     * repository.ontology.owl.csv.dir=storm/plugins/csv/ontology/
     * </pre></code>
     *
     * And the following class.
     *
     * <pre><code>
     * class OwlItem {
     *   {@literal @}Configurable
     *   public String iri;
     *
     *   {@literal @}Configurable
     *   public String dir;
     * }
     * </core></pre>
     *
     * Would produce a map with two keys "dev" and "csv" mapped to an OwlItem object.
     *
     * @param prefix           The configuration key prefix
     * @param configurableType The type of each configurable object to create instances of
     */
    public <T> Map<String, T> getMultiValueConfigurables(String prefix, Class<T> configurableType) {
        Map<String, Map<String, String>> multiValues = getMultiValue(prefix);
        Map<String, T> results = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : multiValues.entrySet()) {
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
    public Map<String, Map<String, String>> getMultiValue(String prefix) {
        return getMultiValue(this.config.entrySet(), prefix);
    }

    /**
     * Processing configuration items that looks like this:
     * <p></p>
     * <pre><code>
     * repository.ontology.owl.dev.iri=http://bigconnect/dev
     * repository.ontology.owl.dev.dir=examples/ontology-dev/
     *
     * repository.ontology.owl.csv.iri=http://bigconnect/csv
     * repository.ontology.owl.csv.dir=storm/plugins/csv/ontology/
     * </code></pre>
     * <p></p>
     * Into a hash like this:
     * <p></p>
     * - dev
     * - iri: http://bigconnect/dev
     * - dir: examples/ontology-dev/
     * - csv
     * - iri: http://bigconnect/csv
     * - dir: storm/plugins/csv/ontology/
     */
    public static SortedMap<String, Map<String, String>> getMultiValue(Iterable<Map.Entry<String, String>> config, String prefix) {
        if (!prefix.endsWith(".")) {
            prefix = prefix + ".";
        }

        SortedMap<String, Map<String, String>> results = new TreeMap<>();
        for (Map.Entry<String, String> item : config) {
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
                Map<String, String> values = results.get(key);
                if (values == null) {
                    values = new HashMap<>();
                    results.put(key, values);
                }
                values.put(subkey, item.getValue());
            }
        }
        return results;
    }

    public JSONObject getConfigurationInfo() {
        return configurationLoader.getConfigurationInfo();
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
