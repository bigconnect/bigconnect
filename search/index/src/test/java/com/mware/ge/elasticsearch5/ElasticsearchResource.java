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
package com.mware.ge.elasticsearch5;

import com.mware.ge.Graph;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.GraphWithSearchIndex;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.logging.log4j.util.Strings;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.mware.ge.GraphConfiguration.AUTO_FLUSH;
import static com.mware.ge.GraphConfiguration.SEARCH_INDEX_PROP_PREFIX;
import static com.mware.ge.elasticsearch5.DefaultIndexSelectionStrategy.CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX;
import static com.mware.ge.elasticsearch5.DefaultIndexSelectionStrategy.CONFIG_INDEX_NAME;
import static com.mware.ge.elasticsearch5.ElasticsearchSearchIndexConfiguration.*;
import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

public class ElasticsearchResource extends ExternalResource {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(ElasticsearchResource.class);

    private static final String ES_INDEX_NAME = "ge-test";
    private static final String ES_EXTENDED_DATA_INDEX_NAME_PREFIX = "ge-test-";
    public static final int TEST_QUERY_PAGE_SIZE = 30;
    public static final int TEST_QUERY_PAGING_LIMIT = 50;

    private ElasticsearchClusterRunner runner;
    private String clusterName;
    private static Client sharedClient;

    private Map extraConfig = null;

    public ElasticsearchResource(String clusterName) {
        this.clusterName = clusterName;
    }

    public ElasticsearchResource(String clusterName, Map extraConfig) {
        this.clusterName = clusterName;
        this.extraConfig = extraConfig;
    }

    @Override
    protected void before() throws Throwable {
//        System.setProperty("REMOTE_ES_CLUSTER_NAME", "bdl");
//        System.setProperty("REMOTE_ES_ADDRESSES", "localhost");

        buildRunner();
    }

    public void buildRunner() {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        File basePath = new File(tempDir, "ge-test-" + UUID.randomUUID().toString());
        LOGGER.info("base path: %s", basePath);

        LogConfigurator.registerErrorListener();

        if (shouldUseRemoteElasticsearch()) {
            runner = null;
        } else {
            runner = new ElasticsearchClusterRunner();
            runner.onBuild((i, builder) ->
                    builder.put("cluster.name", clusterName)
                            .put("http.type", "netty4")
                            .put("transport.type", "netty4")
            ).build(newConfigs().basePath(basePath.getAbsolutePath()).numOfNode(1));
        }
    }

    private boolean shouldUseRemoteElasticsearch() {
        return Strings.isNotEmpty(getRemoteEsAddresses());
    }

    private String getRemoteEsAddresses() {
        return System.getProperty("REMOTE_ES_ADDRESSES");
    }

    @Override
    protected void after() {
        if (sharedClient != null) {
            sharedClient.close();
            sharedClient = null;
        }
        if (runner != null) {
            try {
                runner.close();
            } catch (IOException ex) {
                LOGGER.error("could not close runner", ex);
            }
            runner.clean();
        }
    }

    public void dropIndices() throws Exception {
        if (sharedClient != null) {
            AdminClient client = sharedClient.admin();
            String[] indices = client.indices().prepareGetIndex().execute().get().indices();
            for (String index : indices) {
                if (index.startsWith(ES_INDEX_NAME) || index.startsWith(ES_EXTENDED_DATA_INDEX_NAME_PREFIX)) {
                    LOGGER.info("deleting test index: %s", index);
                    client.indices().prepareDelete(index).execute().actionGet();
                }
            }
        }
    }

    public void clearIndices(Elasticsearch5SearchIndex searchIndex) throws Exception {
        String[] indices = searchIndex.getClient().admin().indices().prepareGetIndex().execute().get().indices();
        for (String index : indices) {
            if (index.startsWith(ES_INDEX_NAME) || index.startsWith(ES_EXTENDED_DATA_INDEX_NAME_PREFIX)) {
                LOGGER.info("clearing test index: %s", index);
                BulkByScrollResponse response = new DeleteByQueryRequestBuilder(searchIndex.getClient(), DeleteByQueryAction.INSTANCE)
                        .source(index)
                        .filter(QueryBuilders.matchAllQuery())
                        .get();
                LOGGER.info("removed %d documents", response.getDeleted());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public Map createConfig() {
        Map configMap = new HashMap();
        configMap.put(AUTO_FLUSH, false);
        configMap.put(SEARCH_INDEX_PROP_PREFIX, ElasticsearchSearchIndexWithSharedClient.class.getName());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME, ES_INDEX_NAME);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX, ES_EXTENDED_DATA_INDEX_NAME_PREFIX);
        if (shouldUseRemoteElasticsearch()) {
            configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CLUSTER_NAME, System.getProperty("REMOTE_ES_CLUSTER_NAME", "elasticsearch"));
            configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS, System.getProperty("REMOTE_ES_ADDRESSES"));
        } else {
            configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + CLUSTER_NAME, clusterName);
            configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + ES_LOCATIONS, getLocation());
        }
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_SHARDS, Integer.parseInt(System.getProperty("ES_NUMBER_OF_SHARDS", "1")));
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + NUMBER_OF_REPLICAS, Integer.parseInt(System.getProperty("ES_NUMBER_OF_REPLICAS", "0")));
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + DefaultIndexSelectionStrategy.CONFIG_SPLIT_EDGES_AND_VERTICES, true);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + LOG_REQUEST_SIZE_LIMIT, 10000);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + QUERY_PAGE_SIZE, TEST_QUERY_PAGE_SIZE);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + QUERY_PAGING_LIMIT, TEST_QUERY_PAGING_LIMIT);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + MAX_QUERY_STRING_TERMS, 2000);
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + EXCEPTION_HANDLER, TestElasticsearch5ExceptionHandler.class.getName());
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + INDEX_REFRESH_INTERVAL, "30s");
        configMap.put(SEARCH_INDEX_PROP_PREFIX + "." + SIDECAR, false);

        // transport-5.3.3.jar!/org/elasticsearch/transport/client/PreBuiltTransportClient.class:61 likes to sleep on
        // connection close if default or netty4. This speeds up the test by skipping that
        configMap.put(ES_SETTINGS_CONFIG_PREFIX + "transport.type", "netty4");
        configMap.put(ES_SETTINGS_CONFIG_PREFIX + "http.type", "netty4");

        if (extraConfig != null) {
            configMap.putAll(extraConfig);
        }

        return configMap;
    }

    private String getLocation() {
        ClusterStateResponse responsee = runner.node().client().admin().cluster().prepareState().execute().actionGet();
        TransportAddress address = responsee.getState().getNodes().getNodes().values().iterator().next().value.getAddress();
        return "localhost:" + address.address().getPort();
    }

    public boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(SEARCH_INDEX_PROP_PREFIX + "." + INDEX_EDGES, "false");
        return true;
    }

    public ElasticsearchClusterRunner getRunner() {
        return runner;
    }

    // transport-7.4.2.jar!/org/elasticsearch/transport/client/PreBuiltTransportClient.class:134 likes to sleep on
    // connection close if default or netty4. This speeds up the test by re-using the client to skip the close.
    public static class ElasticsearchSearchIndexWithSharedClient extends Elasticsearch5SearchIndex {
        public ElasticsearchSearchIndexWithSharedClient(Graph graph, GraphConfiguration config) {
            super(graph, config);
        }

        @Override
        protected Client createClient(ElasticsearchSearchIndexConfiguration config) {
            if (sharedClient == null) {
                sharedClient = super.createClient(config);
            }
            return sharedClient;
        }

        @Override
        protected void shutdownElasticsearchClient() {
        }
    }
}

