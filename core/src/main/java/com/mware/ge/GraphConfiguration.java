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
package com.mware.ge;

import com.mware.ge.id.IdGenerator;
import com.mware.ge.id.LongIdGenerator;
import com.mware.ge.metric.DropWizardMetricRegistry;
import com.mware.ge.metric.GeMetricRegistry;
import com.mware.ge.search.DefaultSearchIndex;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.serializer.kryo.quickSerializers.QuickKryoGeSerializer;
import com.mware.ge.util.ConfigurationUtils;

import java.time.Duration;
import java.util.Map;

public class GraphConfiguration {
    public static final String IDGENERATOR_PROP_PREFIX = "idgenerator";
    public static final String SEARCH_INDEX_PROP_PREFIX = "search";
    public static final String AUTO_FLUSH = "autoFlush";

    public static final String DEFAULT_IDGENERATOR = LongIdGenerator.class.getName();
    public static final String DEFAULT_SEARCH_INDEX = DefaultSearchIndex.class.getName();
    public static final boolean DEFAULT_AUTO_FLUSH = false;
    public static final String TABLE_NAME_PREFIX = "tableNamePrefix";
    public static final String DEFAULT_TABLE_NAME_PREFIX = "bc";
    public static final String SERIALIZER = "serializer";
    public static final String DEFAULT_SERIALIZER = QuickKryoGeSerializer.class.getName();
    public static final String METRICS_REGISTRY = "metricsRegistry";
    public static final String DEFAULT_METRICS_REGISTRY = DropWizardMetricRegistry.class.getName();

    public static final String STRICT_TYPING = "strictTyping";
    public static final boolean DEFAULT_STRICT_TYPING = false;
    public static final String CREATE_TABLES = "createTables";
    public static final boolean DEFAULT_CREATE_TABLES = true;
    public static final String BACKUP_DIR = "backupDir";

    private final Map<String, Object> config;

    public GraphConfiguration(Map<String, Object> config) {
        this.config = config;
    }

    public void set(String key, Object value) {
        this.config.put(key, value);
    }

    public Map getConfig() {
        return config;
    }

    @SuppressWarnings("unused")
    public Object getConfig(String key, Object defaultValue) {
        Object o = getConfig().get(key);
        if (o == null) {
            return defaultValue;
        }
        return o;
    }

    public IdGenerator createIdGenerator(Graph graph) throws GeException {
        return ConfigurationUtils.createProvider(graph, this, IDGENERATOR_PROP_PREFIX, DEFAULT_IDGENERATOR);
    }

    public SearchIndex createSearchIndex(Graph graph) throws GeException {
        return ConfigurationUtils.createProvider(graph, this, SEARCH_INDEX_PROP_PREFIX, DEFAULT_SEARCH_INDEX);
    }

    public GeSerializer createSerializer(Graph graph) throws GeException {
        return ConfigurationUtils.createProvider(graph, this, SERIALIZER, DEFAULT_SERIALIZER);
    }

    public GeSerializer createSerializer() throws GeException {
        return ConfigurationUtils.createProvider(null, this, SERIALIZER, DEFAULT_SERIALIZER);
    }

    public GeMetricRegistry createMetricsRegistry() {
        return ConfigurationUtils.createProvider(null, this, METRICS_REGISTRY, DEFAULT_METRICS_REGISTRY);
    }

    public boolean getBoolean(String configKey, boolean defaultValue) {
        return ConfigurationUtils.getBoolean(config, configKey, defaultValue);
    }

    public double getDouble(String configKey, double defaultValue) {
        return ConfigurationUtils.getDouble(config, configKey, defaultValue);
    }

    public int getInt(String configKey, int defaultValue) {
        return ConfigurationUtils.getInt(config, configKey, defaultValue);
    }

    public Integer getInteger(String configKey, Integer defaultValue) {
        return ConfigurationUtils.getInteger(config, configKey, defaultValue);
    }

    public Duration getDuration(String key, Duration defaultValue) {
        return ConfigurationUtils.getDuration(config, key, defaultValue);
    }

    public Duration getDuration(String key, String defaultValue) {
        return ConfigurationUtils.getDuration(config, key, defaultValue);
    }

    public long getConfigLong(String key, long defaultValue) {
        return ConfigurationUtils.getConfigLong(config, key, defaultValue);
    }

    public String getString(String configKey, String defaultValue) {
        return ConfigurationUtils.getString(config, configKey, defaultValue);
    }

    public String getTableNamePrefix() {
        return getString(TABLE_NAME_PREFIX, DEFAULT_TABLE_NAME_PREFIX);
    }

    public boolean isStrictTyping() {
        return getBoolean(STRICT_TYPING, DEFAULT_STRICT_TYPING);
    }

    public boolean isCreateTables() {
        return getBoolean(CREATE_TABLES, DEFAULT_CREATE_TABLES);
    }
}
