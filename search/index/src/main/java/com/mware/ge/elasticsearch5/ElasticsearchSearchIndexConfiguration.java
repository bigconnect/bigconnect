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

import com.mware.core.config.options.GraphOptions;
import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.elasticsearch5.lucene.DefaultQueryStringTransformer;
import com.mware.ge.elasticsearch5.lucene.QueryStringTransformer;
import com.mware.ge.util.ConfigurationUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.unit.TimeValue;

import java.time.Duration;
import java.util.*;

import static com.mware.ge.elasticsearch5.ElasticsearchOptions.ES_SETTINGS_CONFIG_PREFIX;
import static com.mware.ge.elasticsearch5.ElasticsearchOptions.ES_TRANSPORT_CLIENT_PLUGIN_CONFIG_PREFIX;

public class ElasticsearchSearchIndexConfiguration {
    private GraphConfiguration configuration;
    private IndexSelectionStrategy indexSelectionStrategy;
    private QueryStringTransformer queryStringTransformer;

    public ElasticsearchSearchIndexConfiguration(Graph graph, GraphConfiguration graphConfiguration) {
        this.configuration = graphConfiguration;
        this.indexSelectionStrategy = new DefaultIndexSelectionStrategy(graphConfiguration);
        this.queryStringTransformer = new DefaultQueryStringTransformer(graph);
    }

    public GraphConfiguration getGraphConfiguration() {
        return configuration;
    }

    public IndexSelectionStrategy getIndexSelectionStrategy() {
        return indexSelectionStrategy;
    }

    public QueryStringTransformer getQueryStringTransformer() {
        return queryStringTransformer;
    }

    public void setQueryStringTransformer(QueryStringTransformer queryStringTransformer) {
        this.queryStringTransformer = queryStringTransformer;
    }

    public boolean isAutoFlush() {
        return configuration.get(GraphOptions.AUTO_FLUSH);
    }

    public boolean isIndexEdges() {
        return configuration.get(ElasticsearchOptions.INDEX_EDGES);
    }

    public String[] getEsLocations() {
        String esLocationsString = configuration.get(ElasticsearchOptions.ES_LOCATIONS);
        if (esLocationsString == null) {
            throw new GeException(ElasticsearchOptions.ES_LOCATIONS.name() + " is a required configuration parameter");
        }
        return esLocationsString.split(",");
    }

    public String getClusterName() {
        return configuration.get(ElasticsearchOptions.CLUSTER_NAME);
    }

    public int getPort() {
        return configuration.get(ElasticsearchOptions.PORT);
    }

    public int getNumberOfShards() {
        return configuration.get(ElasticsearchOptions.NUMBER_OF_SHARDS);
    }

    public int getNumberOfReplicas() {
        return configuration.get(ElasticsearchOptions.NUMBER_OF_REPLICAS);
    }

    public String getIndexRefreshInterval() {
        return configuration.get(ElasticsearchOptions.INDEX_REFRESH_INTERVAL);
    }

    public int getTermAggregationShardSize() {
        return configuration.get(ElasticsearchOptions.TERM_AGGREGATION_SHARD_SIZE);
    }

    public int getQueryPageSize() {
        return configuration.get(ElasticsearchOptions.QUERY_PAGE_SIZE);
    }

    public int getPagingLimit() {
        return configuration.get(ElasticsearchOptions.QUERY_PAGING_LIMIT);
    }

    public TimeValue getScrollKeepAlive() {
        String value = configuration.get(ElasticsearchOptions.QUERY_SCROLL_KEEP_ALIVE);
        return TimeValue.parseTimeValue(value, null, "");
    }

    public String getGeoShapePrecision() {
        return configuration.get(ElasticsearchOptions.GEOSHAPE_PRECISION);
    }

    public Double getGeoShapeErrorPct() {
        return configuration.get(ElasticsearchOptions.GEOSHAPE_ERROR_PCT);
    }

    public double getGeocircleToPolygonSideLength() {
        return configuration.get(ElasticsearchOptions.GEOCIRCLE_TO_POLYGON_SIDE_LENGTH);
    }

    public int getGeocircleToPolygonMaxNumSides() {
        return configuration.get(ElasticsearchOptions.GEOCIRCLE_TO_POLYGON_MAX_NUM_SIDES);
    }

    public Elasticsearch5ExceptionHandler getExceptionHandler(Graph graph) {
        String className = configuration.get(ElasticsearchOptions.EXCEPTION_HANDLER);
        if (StringUtils.isEmpty(className)) {
            return null;
        }
        return ConfigurationUtils.createInstance(className, graph, configuration);
    }

    public Map<String, String> getEsSettings() {
        Map<String, String> results = new HashMap<>();
        for (Object o : configuration.getConfig().entrySet()) {
            Map.Entry mapEntry = (Map.Entry) o;
            if (!(mapEntry.getKey() instanceof String) || !(mapEntry.getValue() instanceof String)) {
                continue;
            }
            String key = (String) mapEntry.getKey();
            if (key.startsWith(ES_SETTINGS_CONFIG_PREFIX)) {
                String configName = key.substring(ES_SETTINGS_CONFIG_PREFIX.length());
                results.put(configName, (String) mapEntry.getValue());
            }
        }
        return results;
    }

    public Collection<String> getEsPluginClassNames() {
        List<String> results = new ArrayList<>();
        for (Object o : configuration.getConfig().entrySet()) {
            Map.Entry mapEntry = (Map.Entry) o;
            if (!(mapEntry.getKey() instanceof String) || !(mapEntry.getValue() instanceof String)) {
                continue;
            }
            String key = (String) mapEntry.getKey();
            if (key.startsWith(ES_TRANSPORT_CLIENT_PLUGIN_CONFIG_PREFIX)) {
                results.add((String) mapEntry.getValue());
            }
        }
        return results;
    }

    public Integer getLogRequestSizeLimit() {
        return configuration.get(ElasticsearchOptions.LOG_REQUEST_SIZE_LIMIT);
    }

    public int getMaxQueryStringTerms() {
        return configuration.get(ElasticsearchOptions.MAX_QUERY_STRING_TERMS);
    }

    public int getBulkPoolSize() {
        return configuration.get(ElasticsearchOptions.BULK_POOL_SIZE);
    }

    public int getBulkBacklogSize() {
        return configuration.get(ElasticsearchOptions.BULK_BACKLOG_SIZE);
    }

    public int getBulkMaxBatchSize() {
        return configuration.get(ElasticsearchOptions.BULK_MAX_BATCH_SIZE);
    }

    public int getBulkMaxBatchSizeInBytes() {
        return configuration.get(ElasticsearchOptions.BULK_MAX_BATCH_SIZE_IN_BYTES);
    }

    public Duration getBulkBatchWindowTime() {
        return configuration.get(ElasticsearchOptions.BULK_BATCH_WINDOW_TIME);
    }

    public int getBulkMaxFailCount() {
        return configuration.get(ElasticsearchOptions.BULK_MAX_FAIL_COUNT);
    }

    public Duration getBulkRequestTimeout() {
        return configuration.get(ElasticsearchOptions.BULK_REQUEST_TIMEOUT);
    }

    public boolean getRefreshIndexOnFlush() {
        return configuration.get(ElasticsearchOptions.REFRESH_INDEX_ON_FLUSH);
    }

    public boolean sidecarEnabled() {
        return configuration.get(ElasticsearchOptions.SIDECAR_ENABLED);
    }

    public String getSidecarPath() {
        return configuration.get(ElasticsearchOptions.SIDECAR_PATH);
    }
}
