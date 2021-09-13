package com.mware.core.config;

import com.mware.core.exception.BcException;
import com.mware.core.util.ClassUtil;
import com.mware.core.util.ServiceLoaderUtil;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.Preconditions;

import java.util.*;

public abstract class TypedConfiguration {
    protected static GeLogger LOGGER = GeLoggerFactory.getLogger(TypedConfiguration.class);
    protected final Map<String, Object> config;

    public TypedConfiguration(Map<String, Object> config) {
        this.config = config;
    }

    public <T, R> R get(TypedOption<T, R> option) {
        return get(option.name(), option.defaultValue());
    }

    public <T> T get(String propertyKey, T defaultValue) {
        return (T) config.getOrDefault(propertyKey, defaultValue);
    }

    public void set(String key, Object value) {
        if (!OptionSpace.containKey(key)) {
            if (!key.equals(FileConfigurationLoader.ENV_BC_DIR)
                    && !key.startsWith(ServiceLoaderUtil.CONFIG_DISABLE_PREFIX)
                    && !key.equals("processType")) {
                LOGGER.warn("Encountered an undefined config property: %s", key);
            }
            config.put(key, value);
        } else {
            Preconditions.checkArgument(value instanceof String,
                    "Invalid value for key '%s': %s", key, value);
            TypedOption<?, ?> typedOption = OptionSpace.get(key);
            config.put(key, typedOption.parseConvert((String) value));
        }
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

    public Map<String, Object> getSubset(String keyPrefix) {
        Map<String, Object> subset = new HashMap<>();
        for (Map.Entry<String, Object> entry : this.config.entrySet()) {
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

    public Map<String, Object> toMap() {
        return this.config;
    }

    public Iterable<String> getKeys() {
        return this.config.keySet();
    }

    public Iterable<String> getKeys(String keyPrefix) {
        Set<String> keys = new TreeSet<>();
        for (String key : getKeys()) {
            if (key.startsWith(keyPrefix)) {
                keys.add(key);
            }
        }
        return keys;
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
                sb.append(System.getProperty("line.separator"));
            }
            if (key.toLowerCase().contains("password")) {
                sb.append(key).append(": ********");
            } else {
                sb.append(key).append(": ").append(get(key, (String) null));
            }
        }

        return sb.toString();
    }
}
