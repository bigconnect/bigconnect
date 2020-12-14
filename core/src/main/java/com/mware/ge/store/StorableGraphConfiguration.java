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
package com.mware.ge.store;

import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.GraphConfiguration;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.store.util.StreamingPropertyValueStorageStrategy;
import com.mware.ge.util.ConfigurationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public abstract class StorableGraphConfiguration extends GraphConfiguration {
    public static final String STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY_PREFIX = "streamingPropertyValueStorageStrategy";
    public static final String DEFAULT_STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY = FilesystemSPVStorageStrategy.class.getName();
    public static final String ELEMENT_CACHE_ENABLED = "elementCacheEnabled";
    public static final String ELEMENT_CACHE_SIZE = "elementCacheSize";
    public static final String HISTORY_IN_SEPARATE_TABLE = "historyInSeparateTable";
    public static final String ZOOKEEPER_SERVERS = "zookeeperServers";

    public static final String STREAMING_PROPERTY_VALUE_DATA_FOLDER = "spvFolder";
    public static final String DEFAULT_STREAMING_PROPERTY_VALUE_DATA_FOLDER = "/data";
    public static final boolean DEFAULT_ELEMENT_CACHE_ENABLED = false;
    public static final int DEFAULT_ELEMENT_CACHE_SIZE = 1_000_000;
    public static final boolean DEFAULT_HISTORY_IN_SEPARATE_TABLE = false;
    public static final String DEFAULT_ZOOKEEPER_SERVERS = "localhost";

    protected final GeSerializer serializer;

    public StorableGraphConfiguration(Map<String, Object> config) {
        super(config);

        serializer = createSerializer();
    }

    public GeSerializer getSerializer() {
        return serializer;
    }

    public StreamingPropertyValueStorageStrategy createStreamingPropertyValueStorageStrategy(Graph graph) {
        return ConfigurationUtils.createProvider(graph, this, STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY_PREFIX, DEFAULT_STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY);
    }

    public Path createSPVFolder() {
        String folderPath = getString(STREAMING_PROPERTY_VALUE_DATA_FOLDER, DEFAULT_STREAMING_PROPERTY_VALUE_DATA_FOLDER);
        Path path = Paths.get(folderPath);
        if (!path.toFile().exists()) {
            try {
                Files.createDirectory(path);
            } catch (IOException e) {
                throw new GeException(String.format("Could not create SPV storage folder: %s", folderPath), e);
            }
        }
        return path;
    }

    public boolean isElementCacheEnabled() {
        return getBoolean(ELEMENT_CACHE_ENABLED, DEFAULT_ELEMENT_CACHE_ENABLED);
    }

    public int getElementCacheSize() {
        return getInteger(ELEMENT_CACHE_SIZE, DEFAULT_ELEMENT_CACHE_SIZE);
    }

    public boolean isHistoryInSeparateTable() {
        return getBoolean(HISTORY_IN_SEPARATE_TABLE, DEFAULT_HISTORY_IN_SEPARATE_TABLE);
    }

    public String getZookeeperServers() {
        return getString(ZOOKEEPER_SERVERS, DEFAULT_ZOOKEEPER_SERVERS);
    }
}
