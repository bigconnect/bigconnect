package com.mware.ge.accumulo;

import com.mware.core.config.ConfigOption;
import com.mware.core.config.OptionHolder;

import static com.mware.core.config.OptionChecker.disallowEmpty;
import static com.mware.core.config.OptionChecker.positiveInt;

public class AccumuloOptions extends OptionHolder {
    public static final ConfigOption<String> ACCUMULO_INSTANCE_NAME = new ConfigOption<>(
            "graph.accumuloInstanceName",
            "Username to connect to HDFS",
            disallowEmpty(),
            "hdfs"
    );

    public static final ConfigOption<String> ACCUMULO_USERNAME = new ConfigOption<>(
            "graph.username",
            "Username to connect to Accumulo",
            disallowEmpty(),
            "root"
    );

    public static final ConfigOption<String> ACCUMULO_PASSWORD = new ConfigOption<>(
            "graph.password",
            "Password to connect to Accumulo",
            disallowEmpty(),
            "secret"
    );

    public static final ConfigOption<Integer> ACCUMULO_MAX_VERSIONS = new ConfigOption<>(
            "graph.maxVersions",
            "Maximum versions to store for an element",
            positiveInt(),
            1
    );

    public static final ConfigOption<Integer> ACCUMULO_MAX_EXTENDED_DATA_VERSIONS = new ConfigOption<>(
            "graph.maxVersions",
            "Maximum versions to store for extended data element",
            positiveInt(),
            1
    );

    public static final ConfigOption<Long> MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE = new ConfigOption<>(
            "graph.maxStreamingPropertyValueTableDataSize",
            "Maximum length in bytes before doing an overflow to HDFS for streaming properties",
            positiveInt(),
            10 * 1024 * 1024L // 10 Mb
    );

    public static final ConfigOption<String> HDFS_USER = new ConfigOption<>(
            "graph.hdfs.user",
            "Username to connect to HDFS",
            disallowEmpty(),
            "hdfs"
    );

    public static final ConfigOption<String> HADOOP_CONF_DIR = new ConfigOption<>(
            "graph.hdfs.confDir",
            "Location of Hadoop config files",
            ""
    );

    public static final ConfigOption<String> HDFS_ROOT_DIR = new ConfigOption<>(
            "graph.hdfs.rootDir",
            "HDFS root folder",
            ""
    );

    public static final ConfigOption<String> HDFS_DATA_DIR = new ConfigOption<>(
            "graph.hdfs.dataDir",
            "HDFS data folder",
            "/bigconnect"
    );


    public static final ConfigOption<String> HDFS_CONTEXT_CLASSPATH = new ConfigOption<>(
            "graph.hdfsContextClasspath",
            "",
            ""
    );

    public static final ConfigOption<Long> BATCHWRITER_MAX_MEMORY = new ConfigOption<>(
            "graph.batchwriter.maxMemory",
            "",
            positiveInt(),
            100 * 1024 * 1024L // 100 Mb
    );

    public static final ConfigOption<Long> BATCHWRITER_MAX_LATENCY = new ConfigOption<>(
            "graph.batchwriter.maxLatency",
            "",
            positiveInt(),
            2 * 60 * 1000L // 2 min
    );

    public static final ConfigOption<Long> BATCHWRITER_TIMEOUT = new ConfigOption<>(
            "graph.batchwriter.timeout",
            "",
            positiveInt(),
            Long.MAX_VALUE
    );

    public static final ConfigOption<Integer> BATCHWRITER_MAX_WRITE_THREADS = new ConfigOption<>(
            "graph.batchwriter.maxWriteThreads",
            "",
            positiveInt(),
            Runtime.getRuntime().availableProcessors()
    );

    public static final ConfigOption<Integer> NUMBER_OF_QUERY_THREADS = new ConfigOption<>(
            "graph.numberOfQueryThreads",
            "",
            positiveInt(),
            Runtime.getRuntime().availableProcessors()
    );

    public static final ConfigOption<Integer> LARGE_VALUE_ERROR_THRESHOLD = new ConfigOption<>(
            "graph.largeValueErrorThreshold",
            "",
            positiveInt(),
            500 * 1024 * 1024 // 500Mb
    );

    public static final ConfigOption<Integer> LARGE_VALUE_WARNING_THRESHOLD = new ConfigOption<>(
            "graph.largeValueErrorThreshold",
            "",
            positiveInt(),
            100 * 1024 * 1024 // 100Mb
    );

    public static final ConfigOption<Boolean> COMPRESS_ITERATOR_TRANSFERS = new ConfigOption<>(
            "graph.compressIteratorTransfers",
            "",
            disallowEmpty(),
            true
    );

    private AccumuloOptions() {
        super();
    }

    private static volatile AccumuloOptions instance;

    public static synchronized AccumuloOptions instance() {
        if (instance == null) {
            instance = new AccumuloOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }
}
