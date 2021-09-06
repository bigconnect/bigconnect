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

import com.mware.ge.*;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DefaultIndexSelectionStrategy implements IndexSelectionStrategy {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(DefaultIndexSelectionStrategy.class);
    public static final String CONFIG_INDEX_NAME = "indexName";
    public static final String DEFAULT_INDEX_NAME = ".ge";
    public static final String CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX = "extendedDataIndexNamePrefix";
    public static final String DEFAULT_EXTENDED_DATA_INDEX_NAME_PREFIX = ".ge_extdata_";
    public static final String CONFIG_SPLIT_EDGES_AND_VERTICES = "splitEdgesAndVertices";
    public static final boolean DEFAULT_SPLIT_VERTICES_AND_EDGES = true;
    private static final long INDEX_UPDATE_MS = 5 * 60 * 1000;
    public static final String VERTICES_INDEX_SUFFIX_NAME = "-vertices";
    public static final String EDGES_INDEX_SUFFIX_NAME = "-edges";
    private final String defaultIndexName;
    private final String extendedDataIndexNamePrefix;
    private final ReadWriteLock indicesToQueryLock = new ReentrantReadWriteLock();
    private final boolean splitEdgesAndVertices;
    private String[] indicesToQueryArray;
    private long nextUpdateTime;

    public DefaultIndexSelectionStrategy(GraphConfiguration config) {
        defaultIndexName = getDefaultIndexName(config);
        extendedDataIndexNamePrefix = getExtendedDataIndexNamePrefix(config);
        splitEdgesAndVertices = getSplitEdgesAndVertices(config);
    }

    private static String getDefaultIndexName(GraphConfiguration config) {
        String defaultIndexName = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_INDEX_NAME, DEFAULT_INDEX_NAME);
        LOGGER.debug("Default index name: %s", defaultIndexName);
        return defaultIndexName;
    }

    private static String getExtendedDataIndexNamePrefix(GraphConfiguration config) {
        String prefix = config.getString(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_EXTENDED_DATA_INDEX_NAME_PREFIX, DEFAULT_EXTENDED_DATA_INDEX_NAME_PREFIX);
        LOGGER.debug("Extended data index name prefix: %s", prefix);
        return prefix;
    }

    private static boolean getSplitEdgesAndVertices(GraphConfiguration config) {
        boolean splitEdgesAndVertices = config.getBoolean(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX + "." + CONFIG_SPLIT_EDGES_AND_VERTICES, DEFAULT_SPLIT_VERTICES_AND_EDGES);
        LOGGER.debug("Split edges and vertices: %s", splitEdgesAndVertices);
        return splitEdgesAndVertices;
    }

    private void invalidateIndiciesToQueryCache() {
        nextUpdateTime = 0;
    }

    @Override
    public String[] getIndicesToQuery(Elasticsearch5SearchIndex es) {
        Lock readLock = indicesToQueryLock.readLock();
        readLock.lock();
        try {
            if (indicesToQueryArray != null && new Date().getTime() <= nextUpdateTime) {
                return indicesToQueryArray;
            }
        } finally {
            readLock.unlock();
        }
        loadIndicesToQuery(es);
        return indicesToQueryArray;
    }

    private void loadIndicesToQuery(Elasticsearch5SearchIndex es) {
        Set<String> newIndicesToQuery = new HashSet<>();
        if (splitEdgesAndVertices) {
            newIndicesToQuery.add(defaultIndexName + VERTICES_INDEX_SUFFIX_NAME);
            newIndicesToQuery.add(defaultIndexName + EDGES_INDEX_SUFFIX_NAME);
        } else {
            newIndicesToQuery.add(defaultIndexName);
        }
        Set<String> indexNames = es.getIndexNamesFromElasticsearch();
        for (String indexName : indexNames) {
            if (indexName.startsWith(extendedDataIndexNamePrefix)) {
                newIndicesToQuery.add(indexName);
            }
        }

        for (String indexName : newIndicesToQuery) {
            es.ensureIndexCreatedAndInitialized(indexName);
        }

        Lock writeLock = indicesToQueryLock.writeLock();
        writeLock.lock();
        try {
            indicesToQueryArray = newIndicesToQuery.toArray(new String[0]);
            nextUpdateTime = new Date().getTime() + INDEX_UPDATE_MS;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String getIndexName(Elasticsearch5SearchIndex es, ElementId elementId) {
        if (splitEdgesAndVertices) {
            if (elementId.getElementType() == ElementType.VERTEX) {
                return defaultIndexName + VERTICES_INDEX_SUFFIX_NAME;
            } else if (elementId.getElementType() == ElementType.EDGE) {
                return defaultIndexName + EDGES_INDEX_SUFFIX_NAME;
            } else {
                throw new GeException("Unhandled element type: " + elementId.getElementType());
            }
        }
        return defaultIndexName;
    }

    @Override
    public String getExtendedDataIndexName(
            Elasticsearch5SearchIndex es,
            ElementLocation elementLocation,
            String tableName,
            String rowId
    ) {
        return getExtendedDataIndexName(es, tableName);
    }

    @Override
    public String getExtendedDataIndexName(Elasticsearch5SearchIndex es, ExtendedDataRowId rowId) {
        return getExtendedDataIndexName(es, rowId.getTableName());
    }

    private String getExtendedDataIndexName(Elasticsearch5SearchIndex es, String tableName) {
        String cleanTableName = tableName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
        String extendedDataIndexName = extendedDataIndexNamePrefix + cleanTableName;
        if (!isIncluded(es, extendedDataIndexName)) {
            invalidateIndiciesToQueryCache();
        }
        return extendedDataIndexName;
    }

    @Override
    public String[] getIndexNames(Elasticsearch5SearchIndex es, PropertyDefinition propertyDefinition) {
        return getIndicesToQuery(es);
    }

    @Override
    public boolean isIncluded(Elasticsearch5SearchIndex es, String indexName) {
        for (String i : getIndicesToQuery(es)) {
            if (i.equals(indexName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String[] getManagedIndexNames(Elasticsearch5SearchIndex es) {
        return getIndicesToQuery(es);
    }

    @Override
    public String[] getIndicesToQuery(ElasticsearchSearchQueryBase query, EnumSet<ElasticsearchDocumentType> elementType) {
        return getIndicesToQuery(query.getSearchIndex());
    }
}
