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
package com.mware.ge.elasticsearch5.bulk;

import com.mware.ge.elasticsearch5.ElasticsearchOptions;

import java.time.Duration;

public class BulkUpdateServiceConfiguration {
    public static final Integer LOG_REQUEST_SIZE_LIMIT_DEFAULT = null;
    private int backlogSize = ElasticsearchOptions.BULK_BACKLOG_SIZE.defaultValue();
    private Duration batchWindowTime = ElasticsearchOptions.BULK_BATCH_WINDOW_TIME.defaultValue();
    private Duration bulkRequestTimeout = ElasticsearchOptions.BULK_REQUEST_TIMEOUT.defaultValue();
    private Integer logRequestSizeLimit = LOG_REQUEST_SIZE_LIMIT_DEFAULT;
    private int maxBatchSize = ElasticsearchOptions.BULK_MAX_BATCH_SIZE.defaultValue();
    private int maxBatchSizeInBytes = ElasticsearchOptions.BULK_MAX_BATCH_SIZE_IN_BYTES.defaultValue();
    private int maxFailCount = ElasticsearchOptions.BULK_MAX_FAIL_COUNT.defaultValue();
    private int poolSize = ElasticsearchOptions.BULK_POOL_SIZE.defaultValue();

    public int getBacklogSize() {
        return backlogSize;
    }

    public BulkUpdateServiceConfiguration setBacklogSize(int backlogSize) {
        this.backlogSize = backlogSize;
        return this;
    }

    public Duration getBatchWindowTime() {
        return batchWindowTime;
    }

    public BulkUpdateServiceConfiguration setBatchWindowTime(Duration batchWindowTime) {
        this.batchWindowTime = batchWindowTime;
        return this;
    }

    public Duration getBulkRequestTimeout() {
        return bulkRequestTimeout;
    }

    public BulkUpdateServiceConfiguration setBulkRequestTimeout(Duration bulkRequestTimeout) {
        this.bulkRequestTimeout = bulkRequestTimeout;
        return this;
    }

    public Integer getLogRequestSizeLimit() {
        return logRequestSizeLimit;
    }

    public BulkUpdateServiceConfiguration setLogRequestSizeLimit(Integer logRequestSizeLimit) {
        this.logRequestSizeLimit = logRequestSizeLimit;
        return this;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public BulkUpdateServiceConfiguration setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    public int getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public BulkUpdateServiceConfiguration setMaxBatchSizeInBytes(int maxBatchSizeInBytes) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        return this;
    }

    public int getMaxFailCount() {
        return maxFailCount;
    }

    public BulkUpdateServiceConfiguration setMaxFailCount(int maxFailCount) {
        this.maxFailCount = maxFailCount;
        return this;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public BulkUpdateServiceConfiguration setPoolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }
}
