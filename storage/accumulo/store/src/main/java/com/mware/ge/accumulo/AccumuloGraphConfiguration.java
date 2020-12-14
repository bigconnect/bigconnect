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

import com.mware.ge.GeException;
import com.mware.ge.accumulo.util.OverflowIntoHdfsStreamingPropertyValueStorageStrategy;
import com.mware.ge.store.StorableGraphConfiguration;
import com.mware.ge.store.util.StreamingPropertyValueStorageStrategy;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import com.mware.ge.Graph;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.id.IdentityNameSubstitutionStrategy;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.util.ConfigurationUtils;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AccumuloGraphConfiguration extends StorableGraphConfiguration {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(AccumuloGraphConfiguration.class);

    public static final String HDFS_CONFIG_PREFIX = "hdfs";
    public static final String BATCHWRITER_CONFIG_PREFIX = "batchwriter";

    public static final String ACCUMULO_INSTANCE_NAME = "accumuloInstanceName";
    public static final String ACCUMULO_USERNAME = "username";
    public static final String ACCUMULO_PASSWORD = "password";
    public static final String ACCUMULO_MAX_VERSIONS = "maxVersions";
    public static final String ACCUMULO_MAX_EXTENDED_DATA_VERSIONS = "maxExtendedDataVersions";
    public static final String NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX = "nameSubstitutionStrategy";
    public static final String MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE = "maxStreamingPropertyValueTableDataSize";
    public static final String HDFS_USER = HDFS_CONFIG_PREFIX + ".user";
    public static final String HDFS_ROOT_DIR = HDFS_CONFIG_PREFIX + ".rootDir";
    public static final String DATA_DIR = HDFS_CONFIG_PREFIX + ".dataDir";
    public static final String BACKUP_DIR = HDFS_CONFIG_PREFIX + ".backupDir";
    public static final String BATCHWRITER_MAX_MEMORY = BATCHWRITER_CONFIG_PREFIX + ".maxMemory";
    public static final String BATCHWRITER_MAX_LATENCY = BATCHWRITER_CONFIG_PREFIX + ".maxLatency";
    public static final String BATCHWRITER_TIMEOUT = BATCHWRITER_CONFIG_PREFIX + ".timeout";
    public static final String BATCHWRITER_MAX_WRITE_THREADS = BATCHWRITER_CONFIG_PREFIX + ".maxWriteThreads";
    public static final String NUMBER_OF_QUERY_THREADS = "numberOfQueryThreads";
    public static final String HDFS_CONTEXT_CLASSPATH = "hdfsContextClasspath";
    public static final String STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY_PREFIX = "streamingPropertyValueStorageStrategy";
    public static final String CLIENT_CONFIGURATION_PROPERTY_CONFIG_PREFIX = "clientConfiguration.";
    public static final String LARGE_VALUE_ERROR_THRESHOLD = "largeValueErrorThreshold";
    public static final String LARGE_VALUE_WARNING_THRESHOLD = "largeValueWarningThreshold";
    public static final String COMPRESS_ITERATOR_TRANSFERS = "compressIteratorTransfers";

    public static final String DEFAULT_ACCUMULO_PASSWORD = "password";
    public static final String DEFAULT_ACCUMULO_USERNAME = "root";
    public static final String DEFAULT_ACCUMULO_INSTANCE_NAME = "accumulo";
    public static final int DEFAULT_MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE = 10 * 1024 * 1024;
    public static final String DEFAULT_HDFS_USER = "hdfs";
    public static final String DEFAULT_HDFS_ROOT_DIR = "";
    public static final String HADOOP_CONF_DIR = HDFS_CONFIG_PREFIX + ".confDir";
    public static final String DEFAULT_BACKUP_DIR = "/bigconnect/backup";
    public static final String DEFAULT_DATA_DIR = "/accumuloGraph";
    private static final String DEFAULT_NAME_SUBSTITUTION_STRATEGY = IdentityNameSubstitutionStrategy.class.getName();
    public static final Long DEFAULT_BATCHWRITER_MAX_MEMORY = 100 * 1024 * 1024L;
    public static final Long DEFAULT_BATCHWRITER_MAX_LATENCY = 2 * 60 * 1000L;
    public static final Long DEFAULT_BATCHWRITER_TIMEOUT = Long.MAX_VALUE;
    public static final Integer DEFAULT_BATCHWRITER_MAX_WRITE_THREADS = 8;
    public static final Integer DEFAULT_ACCUMULO_MAX_VERSIONS = 1;
    public static final int DEFAULT_NUMBER_OF_QUERY_THREADS = 100;
    public static final String DEFAULT_HDFS_CONTEXT_CLASSPATH = null;
    public static final String DEFAULT_STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY = OverflowIntoHdfsStreamingPropertyValueStorageStrategy.class.getName();
    public static final int DEFAULT_LARGE_VALUE_ERROR_THRESHOLD = 500 * 1024 * 1024;
    public static final int DEFAULT_LARGE_VALUE_WARNING_THRESHOLD = 100 * 1024 * 1024;
    public static final boolean DEFAULT_COMPRESS_ITERATOR_TRANSFERS = true;

    public static final String[] HADOOP_CONF_FILENAMES = new String[]{
            "core-site.xml",
            "hdfs-site.xml",
            "mapred-site.xml",
            "yarn-site.xml"
    };

    public AccumuloGraphConfiguration(Map<String, Object> config) {
        super(config);
    }

    public AccumuloGraphConfiguration(Configuration configuration, String prefix) {
        super(toMap(configuration, prefix));
    }

    private static Map<String, Object> toMap(Configuration configuration, String prefix) {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                key = key.substring(prefix.length());
            }
            map.put(key, entry.getValue());
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
                    String.format("Could not connect to Accumulo instance [%s] zookeeper servers [%s]", this.getAccumuloInstanceName(), this.getZookeeperServers()),
                    ex
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
        return FileSystem.get(getHdfsRootDir(), getHadoopConfiguration(), getHdfsUser());
    }

    public String getHdfsUser() {
        return getString(HDFS_USER, DEFAULT_HDFS_USER);
    }

    public URI getHdfsRootDir() throws URISyntaxException {
        return new URI(getString(HDFS_ROOT_DIR, DEFAULT_HDFS_ROOT_DIR));
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
        String hadoopConfDir = getString(HADOOP_CONF_DIR, null);
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
                LOGGER.warn("configuration property %s is not a directory", HADOOP_CONF_DIR);
            }
        }
    }

    public AuthenticationToken getAuthenticationToken() {
        String password = getString(ACCUMULO_PASSWORD, DEFAULT_ACCUMULO_PASSWORD);
        return new PasswordToken(password);
    }

    public String getAccumuloUsername() {
        return getString(ACCUMULO_USERNAME, DEFAULT_ACCUMULO_USERNAME);
    }

    public String getAccumuloInstanceName() {
        return getString(ACCUMULO_INSTANCE_NAME, DEFAULT_ACCUMULO_INSTANCE_NAME);
    }

    public boolean isAutoFlush() {
        return getBoolean(AUTO_FLUSH, DEFAULT_AUTO_FLUSH);
    }

    public long getMaxStreamingPropertyValueTableDataSize() {
        return getConfigLong(MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, DEFAULT_MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE);
    }

    public String getBackupDir() {
        return getString(BACKUP_DIR, DEFAULT_BACKUP_DIR);
    }

    public String getDataDir() {
        return getString(DATA_DIR, DEFAULT_DATA_DIR);
    }

    public NameSubstitutionStrategy createSubstitutionStrategy(Graph graph) {
        NameSubstitutionStrategy strategy = ConfigurationUtils.createProvider(graph, this, NAME_SUBSTITUTION_STRATEGY_PROP_PREFIX, DEFAULT_NAME_SUBSTITUTION_STRATEGY);
        strategy.setup(getConfig());
        return strategy;
    }

    public StreamingPropertyValueStorageStrategy createStreamingPropertyValueStorageStrategy(Graph graph) {
        return ConfigurationUtils.createProvider(graph, this, STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY_PREFIX, DEFAULT_STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY);
    }

    public BatchWriterConfig createBatchWriterConfig() {
        long maxMemory = getConfigLong(BATCHWRITER_MAX_MEMORY, DEFAULT_BATCHWRITER_MAX_MEMORY);
        long maxLatency = getConfigLong(BATCHWRITER_MAX_LATENCY, DEFAULT_BATCHWRITER_MAX_LATENCY);
        int maxWriteThreads = getInt(BATCHWRITER_MAX_WRITE_THREADS, DEFAULT_BATCHWRITER_MAX_WRITE_THREADS);
        long timeout = getConfigLong(BATCHWRITER_TIMEOUT, DEFAULT_BATCHWRITER_TIMEOUT);

        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(maxMemory);
        config.setMaxLatency(maxLatency, TimeUnit.MILLISECONDS);
        config.setMaxWriteThreads(maxWriteThreads);
        config.setTimeout(timeout, TimeUnit.MILLISECONDS);
        return config;
    }

    public Integer getMaxVersions() {
        return getInteger(ACCUMULO_MAX_VERSIONS, DEFAULT_ACCUMULO_MAX_VERSIONS);
    }

    public Integer getExtendedDataMaxVersions() {
        return getInteger(ACCUMULO_MAX_EXTENDED_DATA_VERSIONS, getMaxVersions());
    }

    public int getNumberOfQueryThreads() {
        return getInt(NUMBER_OF_QUERY_THREADS, DEFAULT_NUMBER_OF_QUERY_THREADS);
    }

    public String getHdfsContextClasspath() {
        return getString(HDFS_CONTEXT_CLASSPATH, DEFAULT_HDFS_CONTEXT_CLASSPATH);
    }

    public int getLargeValueErrorThreshold() {
        return getInt(LARGE_VALUE_ERROR_THRESHOLD, DEFAULT_LARGE_VALUE_ERROR_THRESHOLD);
    }

    public int getLargeValueWarningThreshold() {
        return getInt(LARGE_VALUE_WARNING_THRESHOLD, DEFAULT_LARGE_VALUE_WARNING_THRESHOLD);
    }

    public boolean isCompressIteratorTransfers() {
        return getBoolean(COMPRESS_ITERATOR_TRANSFERS, DEFAULT_COMPRESS_ITERATOR_TRANSFERS);
    }
}
