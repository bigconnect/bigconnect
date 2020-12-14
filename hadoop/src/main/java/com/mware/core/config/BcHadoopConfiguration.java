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
package com.mware.core.config;

import org.apache.commons.io.FileUtils;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class BcHadoopConfiguration {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(BcHadoopConfiguration.class);

    public static final String HDFS_LIB_SOURCE_DIRECTORY = "hdfsLib.sourceDirectory";
    public static final String HDFS_LIB_TEMP_DIRECTORY = "hdfsLib.tempDirectory";
    public static final String HDFS_USER_NAME = "hdfsUserName";
    public static final String HDFS_USER_NAME_DEFAULT = "hadoop";

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    public static final String PROPERTY_HADOOP_CONF_DIR = "hadoop.conf.dir";
    public static final String ENV_VARIABLE_HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    public static final String DEFAULT_HADOOP_CONF_DIR = "/etc/hadoop/conf";
    public static final String[] HADOOP_CONF_FILENAMES = new String[]{"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

    @Deprecated
    public static org.apache.hadoop.conf.Configuration toHadoopConfiguration(Configuration configuration) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        for (Object entryObj : configuration.toMap().entrySet()) {
            Map.Entry entry = (Map.Entry) entryObj;
            conf.set(entry.getKey().toString(), entry.getValue().toString());
        }
        return conf;
    }

    @Deprecated
    public static org.apache.hadoop.conf.Configuration toHadoopConfiguration(Configuration configuration, org.apache.hadoop.conf.Configuration additionalConfiguration) {
        org.apache.hadoop.conf.Configuration hadoopConfig = toHadoopConfiguration(configuration);
        hadoopConfig.setBoolean("mapred.used.genericoptionsparser", true); // eliminates warning on our version of hadoop
        for (Map.Entry<String, String> toolConfItem : additionalConfiguration) {
            hadoopConfig.set(toolConfItem.getKey(), toolConfItem.getValue());
        }
        return hadoopConfig;
    }

    public static org.apache.hadoop.conf.Configuration getHadoopConfiguration(Configuration configuration, org.apache.hadoop.conf.Configuration additionalConfiguration) {
        org.apache.hadoop.conf.Configuration result = getHadoopConfiguration(configuration);
        for (Map.Entry<String, String> entry : additionalConfiguration) {
            result.set(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static org.apache.hadoop.conf.Configuration getHadoopConfiguration(Configuration configuration) {
        org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();

        for (Object entryObj : configuration.toMap().entrySet()) {
            Map.Entry entry = (Map.Entry) entryObj;
            hadoopConfiguration.set(entry.getKey().toString(), entry.getValue().toString());
        }
        hadoopConfiguration.setBoolean("mapred.used.genericoptionsparser", true);

        File dir = null;
        String property = configuration.get(PROPERTY_HADOOP_CONF_DIR, null);
        String envVariable = System.getenv(ENV_VARIABLE_HADOOP_CONF_DIR);
        if (property != null) {
            dir = new File(property);
            if (!dir.isDirectory()) {
                LOGGER.warn("configuration property %s is not a directory", PROPERTY_HADOOP_CONF_DIR);
                dir = null;
            }
        }
        if (dir == null && envVariable != null) {
            dir = new File(envVariable);
            if (!dir.isDirectory()) {
                LOGGER.warn("environment variable %s is not a directory", ENV_VARIABLE_HADOOP_CONF_DIR);
                dir = null;
            }
        }
        if (dir == null) {
            dir = new File(DEFAULT_HADOOP_CONF_DIR);
            if (!dir.isDirectory()) {
                LOGGER.warn("(default) %s is not a directory", DEFAULT_HADOOP_CONF_DIR);
                dir = null;
            }
        }
        if (dir != null) {
            for (String xmlFilename : HADOOP_CONF_FILENAMES) {
                File file = new File(dir, xmlFilename);
                if (file.isFile()) {
                    LOGGER.info("adding resource: %s to Hadoop configuration", file);
                    try {
                        ByteArrayInputStream in = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
                        hadoopConfiguration.addResource(in);
                    } catch (Exception ex) {
                        LOGGER.warn("error adding resource: " + xmlFilename + " to Hadoop configuration", ex);
                    }
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        SortedSet<String> keys = new TreeSet<>();
        for (Map.Entry<String, String> entry : hadoopConfiguration) {
            keys.add(entry.getKey());
        }

        boolean first = true;
        for (String key : keys) {
            String[] sources = hadoopConfiguration.getPropertySources(key);
            if (sources == null) {
                continue;
            }
            String source = sources[sources.length - 1];

            if (source.endsWith("default.xml") && !LOGGER.isTraceEnabled()) {
                continue;
            }

            if (first) {
                first = false;
            } else {
                sb.append(LINE_SEPARATOR);
            }
            if (key.toLowerCase().contains("password")) {
                sb.append(key).append(": ********");
            } else {
                sb.append(key).append(": ").append(hadoopConfiguration.get(key));
            }
            sb.append(" (").append(source).append(")");
        }

        LOGGER.debug("Hadoop configuration:%n%s", sb.toString());

        return hadoopConfiguration;
    }
}
