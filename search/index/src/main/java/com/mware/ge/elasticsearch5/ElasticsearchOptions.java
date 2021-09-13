package com.mware.ge.elasticsearch5;

import com.mware.core.config.ConfigOption;
import com.mware.core.config.OptionHolder;

import java.time.Duration;

import static com.mware.core.config.OptionChecker.*;

public class ElasticsearchOptions extends OptionHolder {
    public static final String ES_SETTINGS_CONFIG_PREFIX = "search.esSettings.";
    public static final String ES_TRANSPORT_CLIENT_PLUGIN_CONFIG_PREFIX = "search.esTransportClientPlugin.";

    public static final ConfigOption<String> ES_LOCATIONS = new ConfigOption<>(
            "graph.search.locations",
            "List of comma-separated Elasticsearch hosts",
            disallowEmpty(),
            "localhost"
    );

    public static final ConfigOption<String> CLUSTER_NAME = new ConfigOption<>(
            "graph.search.clusterName",
            "Name of the Elasticsearch cluster",
            disallowEmpty(),
            "bdl"
    );

    public static final ConfigOption<Integer> PORT = new ConfigOption<>(
            "graph.search.port",
            "Elasticsearch port",
            positiveInt(),
            9300
    );

    public static final ConfigOption<Boolean> SIDECAR_ENABLED = new ConfigOption<>(
            "graph.search.sidecar",
            "Start embedded Elasticsearch server",
            true
    );

    public static final ConfigOption<String> SIDECAR_PATH = new ConfigOption<>(
            "graph.search.sidecar.path",
            "Folder to store embedded Elasticsearch data",
            ""
    );

    public static final ConfigOption<Integer> NUMBER_OF_SHARDS = new ConfigOption<>(
            "graph.search.shards",
            "Number of shards to set on index creation (cannot be changed after)",
            positiveInt(),
            Runtime.getRuntime().availableProcessors()
    );

    public static final ConfigOption<Integer> NUMBER_OF_REPLICAS = new ConfigOption<>(
            "graph.search.replicas",
            "Number of replicas to set on index creation",
            nonNegativeInt(),
            1
    );

    public static final ConfigOption<String> SEARCH_INDEX_NAME = new ConfigOption<>(
            "graph.search.indexName",
            "Index name for storing graph elements",
            disallowEmpty(),
            ".ge"
    );

    public static final ConfigOption<String> SEARCH_EXTDATA_INDEX_NAME = new ConfigOption<>(
            "graph.search.extendedDataIndexNamePrefix",
            "Index name prefix for storing extended data",
            disallowEmpty(),
            ".ge_extdata_"
    );

    public static final ConfigOption<Boolean> SEARCH_SPLIT_EDGES_AND_VERTICES = new ConfigOption<>(
            "graph.search.splitEdgesAndVertices",
            "Whether to create separate indexes for storing edges and vertices",
            disallowEmpty(),
            false
    );

    public static final ConfigOption<Boolean> INDEX_EDGES = new ConfigOption<>(
            "graph.search.indexEdges",
            "Enable edge indexing",
            disallowEmpty(),
            true
    );

    public static final ConfigOption<String> INDEX_REFRESH_INTERVAL = new ConfigOption<>(
            "graph.search.indexRefreshInterval",
            "Elasticsearch index refresh_interval setting",
            disallowEmpty(),
            "1s"
    );

    public static final ConfigOption<Integer> TERM_AGGREGATION_SHARD_SIZE = new ConfigOption<>(
            "graph.search.termAggregation.shardSize",
            "The number of term buckets each shard will return to the coordinating node. The higher the shard size is, the more accurate the results are.",
            disallowEmpty(),
            10
    );

    public static final ConfigOption<Integer> QUERY_PAGE_SIZE = new ConfigOption<>(
            "graph.search.queryPageSize",
            "The number of search results to return in a page.",
            disallowEmpty(),
            500
    );

    public static final ConfigOption<Integer> QUERY_PAGING_LIMIT = new ConfigOption<>(
            "graph.search.queryPagingLimit",
            "Maximum number of pages to switch to Elasticsearch scroll api",
            disallowEmpty(),
            50
    );

    public static final ConfigOption<String> QUERY_SCROLL_KEEP_ALIVE = new ConfigOption<>(
            "graph.search.queryScrollKeepAlive",
            "Enable scrolling of the search request for the specified timeout",
            disallowEmpty(),
            "5m"
    );

    public static final ConfigOption<Integer> MAX_QUERY_STRING_TERMS = new ConfigOption<>(
            "graph.search.maxQueryStringTerms",
            "Maximum number of query terms. Elasticsearch defaults to 1024. " +
                    "For larger values please update elasticsearch.yml: " +
                    "   - index.query.bool.max_clause_count: NNNNN",
            positiveInt(),
            1024
    );

    public static final ConfigOption<String> GEOSHAPE_PRECISION = new ConfigOption<>(
            "graph.search.geoshapePrecision",
            "The value specifies the desired precision and Elasticsearch will calculate the best tree_levels value to honor this precision." +
                    "The value should be a number followed by an optional distance unit." +
                    "Valid distance units include: in, inch, yd, yard, mi, miles, km, kilometers, m,meters, cm,centimeters, mm, millimeters." +
            disallowEmpty(),
            "50m"
    );

    public static final ConfigOption<Double> GEOSHAPE_ERROR_PCT = new ConfigOption<>(
            "graph.search.geoshapeErrorPct",
            "Used as a hint to the PrefixTree about how precise it should be. " +
                    "This can lead to significant memory usage for high resolution shapes." +
                    "Note: This value will default to 0 if GEOSHAPE_PRECISION definition is explicitly defined." +
                    "This guarantees spatial precision at the level defined in the mapping." +
                    "This can lead to significant memory usage for high resolution shapes with low error." +
                    "To improve indexing performance (at the cost of query accuracy) explicitly define GEOSHAPE_PRECISION " +
                    "along with a reasonable GEOSHAPE_ERROR_PCT, noting that large shapes will have greater false positives.",
            rangeDouble(0.0D, 0.5D),
            0.025D
    );

    public static final ConfigOption<Double> GEOCIRCLE_TO_POLYGON_SIDE_LENGTH = new ConfigOption<>(
            "graph.search.geocircleToPolygonSideLengthKm",
            "",
            rangeDouble(0.0D, Double.MAX_VALUE),
            1.0D
    );

    public static final ConfigOption<Integer> GEOCIRCLE_TO_POLYGON_MAX_NUM_SIDES = new ConfigOption<>(
            "graph.search.geocircleToPolygonMaxNumSides",
            "",
            positiveInt(),
            1000
    );

    public static final ConfigOption<Integer> BULK_POOL_SIZE = new ConfigOption<>(
            "graph.search.bulk.poolSize",
            "",
            positiveInt(),
            10
    );

    public static final ConfigOption<Integer> BULK_BACKLOG_SIZE = new ConfigOption<>(
            "graph.search.bulk.backlogSize",
            "",
            positiveInt(),
            100
    );

    public static final ConfigOption<Integer> BULK_MAX_BATCH_SIZE = new ConfigOption<>(
            "graph.search.bulk.maxBatchSize",
            "",
            positiveInt(),
            1000
    );

    public static final ConfigOption<Integer> BULK_MAX_BATCH_SIZE_IN_BYTES = new ConfigOption<>(
            "graph.search.bulk.maxBatchSizeInBytes",
            "",
            positiveInt(),
            10 * 1024 * 1024
    );

    public static final ConfigOption<Duration> BULK_BATCH_WINDOW_TIME = new ConfigOption<>(
            "graph.search.bulk.batchWindowTime",
            "",
            disallowEmpty(),
            Duration.ofMillis(1000)
    );

    public static final ConfigOption<Integer> BULK_MAX_FAIL_COUNT = new ConfigOption<>(
            "graph.search.bulk.maxFailCount",
            "",
            positiveInt(),
            10
    );

    public static final ConfigOption<Duration> BULK_REQUEST_TIMEOUT = new ConfigOption<>(
            "graph.search.bulk.requestTimeout",
            "",
            disallowEmpty(),
            Duration.ofMinutes(30)
    );

    public static final ConfigOption<Boolean> REFRESH_INDEX_ON_FLUSH = new ConfigOption<>(
            "graph.search.refreshIndexOnFlush",
            "Perform an index refresh on flush",
            disallowEmpty(),
            true
    );

    public static final ConfigOption<Integer> LOG_REQUEST_SIZE_LIMIT = new ConfigOption<>(
            "graph.search.logRequestSizeLimit",
            "Log requests that exceed the given size in bytes",
            nonNegativeInt(),
            Integer.MAX_VALUE
    );

    public static final ConfigOption<String> EXCEPTION_HANDLER = new ConfigOption<>(
            "graph.search.exceptionHandler",
            "This is used for testing only",
            ""
    );

    private ElasticsearchOptions() {
        super();
    }

    private static volatile ElasticsearchOptions instance;

    public static synchronized ElasticsearchOptions instance() {
        if (instance == null) {
            instance = new ElasticsearchOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }
}
