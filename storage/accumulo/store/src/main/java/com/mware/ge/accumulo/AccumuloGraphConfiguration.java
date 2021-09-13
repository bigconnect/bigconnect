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
package com.mware.ge.accumulo;

import com.mware.core.config.OptionSpace;
import com.mware.core.config.TypedOption;
import com.mware.core.config.options.GraphOptions;
import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.accumulo.util.OverflowIntoHdfsStreamingPropertyValueStorageStrategy;
import com.mware.ge.id.IdentityNameSubstitutionStrategy;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.store.StorableGraphConfiguration;
import com.mware.ge.store.util.StreamingPropertyValueStorageStrategy;
import com.mware.ge.util.ConfigurationUtils;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class AccumuloGraphConfiguration extends StorableGraphConfiguration {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(AccumuloGraphConfiguration.class);
    public static final String CLIENT_CONFIGURATION_PROPERTY_CONFIG_PREFIX = "clientConfiguration.";
    public static final String DEFAULT_STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY = OverflowIntoHdfsStreamingPropertyValueStorageStrategy.class.getName();

    public static final String[] HADOOP_CONF_FILENAMES = new String[]{
            "core-site.xml",
            "hdfs-site.xml",
            "mapred-site.xml",
            "yarn-site.xml"
    };

    public AccumuloGraphConfiguration(Configuration configuration) {
        this(toMap(configuration));
    }

    public AccumuloGraphConfiguration(Map<String, Object> config) {
        super(config);

        OptionSpace.register("accumulo", AccumuloOptions.instance());

        ((TypedOption<String, String>) OptionSpace.get(GraphOptions.STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY.name()))
                .overrideDefaultValue(DEFAULT_STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY);
    }

    private static Map<String, Object> toMap(Configuration configuration) {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    public Connector createConnector() {
        try {
            LOGGER.info("Connecting to accumulo instance [%s] zookeeper servers [%s]", this.getAccumuloInstanceName(), this.getZookeeperServers());
            ZooKeeperInstance instance = new ZooKeeperInstance(getClientConfiguration());
            return instance.getConnector(this.getAccumuloUsername(), this.getAuthenticationToken());
        } catch (Exception ex) {
            throw new GeException(
                    String.format("Could not connect to Accumulo instance [%s] zookeeper servers [%s]",
                            this.getAccumuloInstanceName(), this.getZookeeperServers()), ex
            );
        }
    }

    public ClientConfiguration getClientConfiguration() {
        ClientConfiguration config = new ClientConfiguration()
                .withInstance(this.getAccumuloInstanceName())
                .withZkHosts(this.getZookeeperServers());

        for (Map.Entry<String, String> entry : getClientConfigurationProperties().entrySet()) {
            config.setProperty(entry.getKey(), entry.getValue());
        }
        return config;
    }

    public Map<String, String> getClientConfigurationProperties() {
        Map<String, String> results = new HashMap<>();
        for (Object o : getConfig().entrySet()) {
            Map.Entry mapEntry = (Map.Entry) o;
            if (!(mapEntry.getKey() instanceof String) || !(mapEntry.getValue() instanceof String)) {
                continue;
            }
            String key = (String) mapEntry.getKey();
            if (key.startsWith(CLIENT_CONFIGURATION_PROPERTY_CONFIG_PREFIX)) {
                String configName = key.substring(CLIENT_CONFIGURATION_PROPERTY_CONFIG_PREFIX.length());
                results.put(configName, (String) mapEntry.getValue());
            }
        }
        return results;
    }

    public FileSystem createFileSystem() throws URISyntaxException, IOException, InterruptedException {
        return FileSystem.get(new URI(getHdfsRootDir()), getHadoopConfiguration(), getHdfsUser());
    }

    public String getHdfsUser() {
        return get(AccumuloOptions.HDFS_USER);
    }

    public String getHdfsRootDir() {
        return get(AccumuloOptions.HDFS_ROOT_DIR);
    }

    public org.apache.hadoop.conf.Configuration getHadoopConfiguration() {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        for (Object entrySetObject : getConfig().entrySet()) {
            Map.Entry entrySet = (Map.Entry) entrySetObject;
            configuration.set("" + entrySet.getKey(), "" + entrySet.getValue());
        }

        loadHadoopConfigs(configuration);
        return configuration;
    }

    private void loadHadoopConfigs(Configuration configuration) {
        String hadoopConfDir = get(AccumuloOptions.HADOOP_CONF_DIR);
        if (hadoopConfDir != null) {
            LOGGER.info("hadoop conf dir", hadoopConfDir);
            File dir = new File(hadoopConfDir);
            if (dir.isDirectory()) {
                for (String xmlFilename : HADOOP_CONF_FILENAMES) {
                    File file = new File(dir, xmlFilename);
                    if (file.isFile()) {
                        LOGGER.info("adding resource: %s to Hadoop configuration", file);
                        try {
                            FileInputStream in = new FileInputStream(file);
                            configuration.addResource(in);
                        } catch (Exception ex) {
                            LOGGER.warn("error adding resource: " + xmlFilename + " to Hadoop configuration", ex);
                        }
                    }
                }

                StringBuilder sb = new StringBuilder();
                SortedSet<String> keys = new TreeSet<>();
                for (Map.Entry<String, String> entry : configuration) {
                    keys.add(entry.getKey());
                }

                LOGGER.debug("Hadoop configuration:%n%s", sb.toString());
            } else {
                LOGGER.warn("configuration property %s is not a directory", AccumuloOptions.HADOOP_CONF_DIR.name());
            }
        }
    }

    public AuthenticationToken getAuthenticationToken() {
        return new PasswordToken(get(AccumuloOptions.ACCUMULO_PASSWORD));
    }

    public String getAccumuloUsername() {
        return get(AccumuloOptions.ACCUMULO_USERNAME);
    }

    public String getAccumuloInstanceName() {
        return get(AccumuloOptions.ACCUMULO_INSTANCE_NAME);
    }

    public long getMaxStreamingPropertyValueTableDataSize() {
        return get(AccumuloOptions.MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE);
    }

    public String getBackupDir() {
        return get(GraphOptions.BACKUP_DIR);
    }

    public String getDataDir() {
        return get(AccumuloOptions.HDFS_DATA_DIR);
    }

    public NameSubstitutionStrategy createSubstitutionStrategy(Graph graph) {
        NameSubstitutionStrategy strategy = new IdentityNameSubstitutionStrategy();
        strategy.setup(getConfig());
        return strategy;
    }

    public StreamingPropertyValueStorageStrategy createStreamingPropertyValueStorageStrategy(Graph graph) {
        return ConfigurationUtils.createInstance(graph, this, GraphOptions.STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY);
    }

    public BatchWriterConfig createBatchWriterConfig() {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(get(AccumuloOptions.BATCHWRITER_MAX_MEMORY));
        config.setMaxLatency(get(AccumuloOptions.BATCHWRITER_MAX_LATENCY), TimeUnit.MILLISECONDS);
        config.setMaxWriteThreads(get(AccumuloOptions.BATCHWRITER_MAX_WRITE_THREADS));
        config.setTimeout(get(AccumuloOptions.BATCHWRITER_TIMEOUT), TimeUnit.MILLISECONDS);
        return config;
    }

    public Integer getMaxVersions() {
        return get(AccumuloOptions.ACCUMULO_MAX_VERSIONS);
    }

    public Integer getExtendedDataMaxVersions() {
        return get(AccumuloOptions.ACCUMULO_MAX_EXTENDED_DATA_VERSIONS);
    }

    public int getNumberOfQueryThreads() {
        return get(AccumuloOptions.NUMBER_OF_QUERY_THREADS);
    }

    public String getHdfsContextClasspath() {
        return get(AccumuloOptions.HDFS_CONTEXT_CLASSPATH);
    }

    public int getLargeValueErrorThreshold() {
        return get(AccumuloOptions.LARGE_VALUE_ERROR_THRESHOLD);
    }

    public int getLargeValueWarningThreshold() {
        return get(AccumuloOptions.LARGE_VALUE_WARNING_THRESHOLD);
    }

    public boolean isCompressIteratorTransfers() {
        return get(AccumuloOptions.COMPRESS_ITERATOR_TRANSFERS);
    }
}
