package com.mware.ge.rocksdb;

import com.mware.core.config.ConfigOption;
import com.mware.core.config.OptionHolder;

import static com.mware.core.config.OptionChecker.disallowEmpty;

public class RocksDBOptions extends OptionHolder {
    public static final ConfigOption<String> DATA_PATH = new ConfigOption<>(
            "graph.dataPath",
            "Folder to store data files",
            String.class,
            null
    );

    public static final ConfigOption<String> WAL_PATH = new ConfigOption<>(
            "graph.walPath",
            "Folder to store WAL files",
            String.class,
            null
    );

    public static final ConfigOption<String> LOG_LEVEL = new ConfigOption<>(
            "graph.logLevel",
            "RocksDB Log level (INFO, WARN, ERROR, etc.)",
            disallowEmpty(),
            String.class,
            "INFO"
    );

    public static final ConfigOption<Boolean> OPTIMIZE_MODE = new ConfigOption<>(
            "graph.optimizeMode",
            "Run RocksDB in 'optimized' mode",
            disallowEmpty(),
            Boolean.class,
            true
    );

    public static final ConfigOption<Boolean> BULK_LOAD = new ConfigOption<>(
            "graph.bulkLoad",
            "Run RocksDB in bulk load mode",
            disallowEmpty(),
            Boolean.class,
            false
    );

    private RocksDBOptions() {
        super();
    }

    private static volatile RocksDBOptions instance;

    public static synchronized RocksDBOptions instance() {
        if (instance == null) {
            instance = new RocksDBOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }
}
