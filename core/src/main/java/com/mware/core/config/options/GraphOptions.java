package com.mware.core.config.options;

import com.mware.core.config.ConfigOption;
import com.mware.core.config.OptionHolder;
import com.mware.ge.id.LongIdGenerator;
import com.mware.ge.search.DefaultSearchIndex;
import com.mware.ge.serializer.kryo.quickSerializers.QuickKryoGeSerializer;
import com.mware.ge.store.FilesystemSPVStorageStrategy;

import static com.mware.core.config.OptionChecker.disallowEmpty;
import static com.mware.core.config.OptionChecker.positiveInt;

public class GraphOptions extends OptionHolder {
    public static final ConfigOption<String> GRAPH_IMPL = new ConfigOption<>(
            "graph",
            "Graph store",
            disallowEmpty(),
            String.class,
            "com.mware.ge.rocksdb.RocksDBGraph"
    );

    public static final ConfigOption<String> ZOOKEEPER_SERVERS = new ConfigOption<>(
            "graph.zookeeperServers",
            "Comma-separated list of server:port",
            disallowEmpty(),
            String.class,
            "localhost"
    );

    public static final ConfigOption<String> TABLE_NAME_PREFIX = new ConfigOption<>(
            "graph.tableNamePrefix",
            "Prefix to created graph store tables",
            disallowEmpty(),
            String.class,
            "bc"
    );

    public static final ConfigOption<Boolean> STRICT_TYPING = new ConfigOption<>(
            "graph.strictTyping",
            "Enforce strict type check when creating graph elements",
            disallowEmpty(),
            Boolean.class,
            false
    );

    public static final ConfigOption<Boolean> CREATE_TABLES = new ConfigOption<>(
            "graph.createTables",
            "Create graph store tables if they don't exist",
            disallowEmpty(),
            Boolean.class,
            true
    );

    public static final ConfigOption<String> SEARCH_IMPL = new ConfigOption<>(
            "graph.search",
            "Search index implementation",
            disallowEmpty(),
            String.class,
            DefaultSearchIndex.class.getName()
    );

    public static final ConfigOption<String> SERIALIZER = new ConfigOption<>(
            "graph.serializer",
            "Value serializer implementation",
            disallowEmpty(),
            String.class,
            QuickKryoGeSerializer.class.getName()
    );

    public static final ConfigOption<Boolean> SERIALIZER_COMPRESSION = new ConfigOption<>(
            "graph.serializer.enableCompression",
            "Compress serialized values to optimize space (may reduce performance)",
            disallowEmpty(),
            Boolean.class,
            false
    );

    public static final ConfigOption<Boolean> ELEMENT_CACHE_ENABLED = new ConfigOption<>(
            "graph.elementCacheEnabled",
            "Enable memory caching of graph elements",
            disallowEmpty(),
            Boolean.class,
            true
    );

    public static final ConfigOption<Boolean> AUTO_FLUSH = new ConfigOption<>(
            "graph.autoFlush",
            "Flush after saving each element",
            disallowEmpty(),
            Boolean.class,
            false
    );

    public static final ConfigOption<Integer> ELEMENT_CACHE_SIZE = new ConfigOption<>(
            "graph.elementCacheSize",
            "How many graph elements to cache",
            positiveInt(),
            Integer.class,
            1_000_000
    );

    public static final ConfigOption<String> ID_GENERATOR = new ConfigOption<>(
            "graph.idgenerator",
            "Which ID generation method to use",
            disallowEmpty(),
            String.class,
            LongIdGenerator.class.getName()
    );

    public static final ConfigOption<String> STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY = new ConfigOption<>(
            "graph.streamingPropertyValueStorageStrategy",
            "Strategy for storing StreamingPropertyValues",
            disallowEmpty(),
            String.class,
            FilesystemSPVStorageStrategy.class.getName()
    );

    public static final ConfigOption<String> STREAMING_PROPERTY_VALUE_DATA_FOLDER = new ConfigOption<>(
            "graph.spvFolder",
            "Folder to store SPV StreamingPropertyValue data",
            disallowEmpty(),
            String.class,
            "/bc_data"
    );

    public static final ConfigOption<Boolean> HISTORY_IN_SEPARATE_TABLE = new ConfigOption<>(
            "graph.historyInSeparateTable",
            "Keep history in seaparate data table",
            disallowEmpty(),
            Boolean.class,
            false
    );

    public static final ConfigOption<String> BACKUP_DIR = new ConfigOption<>(
            "graph.backupDir",
            "Backup folder",
            String.class,
            "/bigconnect/backup"
    );

    private GraphOptions() {
        super();
    }

    private static volatile GraphOptions instance;

    public static synchronized GraphOptions instance() {
        if (instance == null) {
            instance = new GraphOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }
}
