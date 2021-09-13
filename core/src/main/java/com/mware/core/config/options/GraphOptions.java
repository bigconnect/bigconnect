package com.mware.core.config.options;

import com.mware.core.config.ConfigOption;
import com.mware.core.config.OptionHolder;
import com.mware.core.process.DataWorkerRunnerProcess;
import com.mware.core.process.LongRunningProcessRunnerProcess;
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
            "com.mware.ge.rocksdb.RocksDBGraph"
    );

    public static final ConfigOption<String> ZOOKEEPER_SERVERS = new ConfigOption<>(
            "graph.zookeeperServers",
            "Comma-separated list of server:port",
            disallowEmpty(),
            "localhost"
    );

    public static final ConfigOption<String> TABLE_NAME_PREFIX = new ConfigOption<>(
            "graph.tableNamePrefix",
            "Prefix to created graph store tables",
            disallowEmpty(),
            "bc"
    );

    public static final ConfigOption<Boolean> STRICT_TYPING = new ConfigOption<>(
            "graph.strictTyping",
            "Enforce strict type check when creating graph elements",
            disallowEmpty(),
            false
    );

    public static final ConfigOption<Boolean> CREATE_TABLES = new ConfigOption<>(
            "graph.createTables",
            "Create graph store tables if they don't exist",
            disallowEmpty(),
            true
    );

    public static final ConfigOption<String> SEARCH_IMPL = new ConfigOption<>(
            "graph.search",
            "Search index implementation",
            disallowEmpty(),
            DefaultSearchIndex.class.getName()
    );

    public static final ConfigOption<String> SERIALIZER = new ConfigOption<>(
            "graph.serializer",
            "Value serializer implementation",
            disallowEmpty(),
            QuickKryoGeSerializer.class.getName()
    );

    public static final ConfigOption<Boolean> SERIALIZER_COMPRESSION = new ConfigOption<>(
            "graph.serializer.enableCompression",
            "Compress serialized values to optimize space (may reduce performance)",
            disallowEmpty(),
            false
    );

    public static final ConfigOption<Boolean> ELEMENT_CACHE_ENABLED = new ConfigOption<>(
            "graph.elementCacheEnabled",
            "Enable memory caching of graph elements",
            disallowEmpty(),
            true
    );

    public static final ConfigOption<Boolean> AUTO_FLUSH = new ConfigOption<>(
            "graph.autoFlush",
            "Flush after saving each element",
            disallowEmpty(),
            false
    );

    public static final ConfigOption<Integer> ELEMENT_CACHE_SIZE = new ConfigOption<>(
            "graph.elementCacheSize",
            "How many graph elements to cache",
            positiveInt(),
            1_000_000
    );

    public static final ConfigOption<String> ID_GENERATOR = new ConfigOption<>(
            "graph.idgenerator",
            "Which ID generation method to use",
            disallowEmpty(),
            LongIdGenerator.class.getName()
    );

    public static final ConfigOption<String> STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY = new ConfigOption<>(
            "graph.streamingPropertyValueStorageStrategy",
            "Strategy for storing StreamingPropertyValues",
            disallowEmpty(),
            FilesystemSPVStorageStrategy.class.getName()
    );

    public static final ConfigOption<String> STREAMING_PROPERTY_VALUE_DATA_FOLDER = new ConfigOption<>(
            "graph.spvFolder",
            "Folder to store SPV StreamingPropertyValue data",
            disallowEmpty(),
            "/bc_data"
    );

    public static final ConfigOption<Boolean> HISTORY_IN_SEPARATE_TABLE = new ConfigOption<>(
            "graph.historyInSeparateTable",
            "Keep history in seaparate data table",
            disallowEmpty(),
            false
    );

    public static final ConfigOption<String> BACKUP_DIR = new ConfigOption<>(
            "graph.backupDir",
            "Backup folder",
            "/bigconnect/backup"
    );

    public static final ConfigOption<Integer> DW_RUNNER_THREAD_COUNT = new ConfigOption<>(
            DataWorkerRunnerProcess.class.getName()+".threadCount",
            "Number of threads for the processing DataWorker messages",
            Runtime.getRuntime().availableProcessors()
    );

    public static final ConfigOption<Integer> LRP_RUNNER_THREAD_COUNT = new ConfigOption<>(
            LongRunningProcessRunnerProcess.class.getName()+".threadCount",
            "Number of threads for the processing LongRunningProcess messages",
            Runtime.getRuntime().availableProcessors()
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
