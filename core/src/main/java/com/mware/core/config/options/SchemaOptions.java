package com.mware.core.config.options;

import com.mware.core.config.ConfigOption;
import com.mware.core.config.OptionHolder;

import static com.mware.core.config.OptionChecker.rangeInt;

public class SchemaOptions extends OptionHolder {
    public static final ConfigOption<Long> CACHE_MAX_SIZE = new ConfigOption<>(
            "schema.cache.maxSize",
            "Maximum schema objects that should be kept in memory",
            rangeInt(0L, Long.MAX_VALUE),
            Long.class,
            1000L
    );

    private SchemaOptions() {
        super();
    }

    private static volatile SchemaOptions instance;

    public static synchronized SchemaOptions instance() {
        if (instance == null) {
            instance = new SchemaOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }
}
