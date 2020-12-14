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

import java.time.Duration;

public class BulkUpdateServiceConfiguration {
    public static final int BACKLOG_SIZE_DEFAULT = 100;
    public static final Duration BATCH_WINDOW_TIME_DEFAULT = Duration.ofMillis(1000);
    public static final Duration BULK_REQUEST_TIMEOUT_DEFAULT = Duration.ofMinutes(30);
    public static final Integer LOG_REQUEST_SIZE_LIMIT_DEFAULT = null;
    public static final int MAX_BATCH_SIZE_DEFAULT = 1000;
    public static final int MAX_BATCH_SIZE_IN_BYTES_DEFAULT = 10 * 1024 * 1024;
    public static final int MAX_FAIL_COUNT_DEFAULT = 10;
    public static final int POOL_SIZE_DEFAULT = 10;
    private int backlogSize = BACKLOG_SIZE_DEFAULT;
    private Duration batchWindowTime = BATCH_WINDOW_TIME_DEFAULT;
    private Duration bulkRequestTimeout = BULK_REQUEST_TIMEOUT_DEFAULT;
    private Integer logRequestSizeLimit = LOG_REQUEST_SIZE_LIMIT_DEFAULT;
    private int maxBatchSize = MAX_BATCH_SIZE_DEFAULT;
    private int maxBatchSizeInBytes = MAX_BATCH_SIZE_IN_BYTES_DEFAULT;
    private int maxFailCount = MAX_FAIL_COUNT_DEFAULT;
    private int poolSize = POOL_SIZE_DEFAULT;

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
