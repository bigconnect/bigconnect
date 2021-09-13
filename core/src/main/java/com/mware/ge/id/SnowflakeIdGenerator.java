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
package com.mware.ge.id;

import com.mware.core.exception.BcException;

import java.util.Calendar;
import java.util.Date;

public class SnowflakeIdGenerator implements IdGenerator {
    private final IdWorker idWorker;

    public SnowflakeIdGenerator() {
        this.idWorker = new IdWorker(0L, 0L);
    }

    @Override
    public String nextId() {
        if (this.idWorker == null) {
            throw new BcException("Please initialize before using");
        }
        return Long.toString(idWorker.nextId());
    }

    /*
     * Copyright 2010-2012 Twitter, Inc.
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *      http://www.apache.org/licenses/LICENSE-2.0
     * Unless required by applicable law or agreed to in writing,
     * software distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
    private static class IdWorker {
        private long workerId;
        private long datacenterId;
        private long sequence = 0L; // AtomicLong
        private long lastTimestamp = -1L;

        private static final long WORKER_BIT = 5L;
        private static final long MAX_WORKER_ID = -1L ^ (-1L << WORKER_BIT);

        private static final long DC_BIT = 5L;
        private static final long MAX_DC_ID = -1L ^ (-1L << DC_BIT);

        private static final long SEQUENCE_BIT = 12L;
        private static final long SEQUENCE_MASK = -1L ^ (-1L << SEQUENCE_BIT);

        private static final long WORKER_SHIFT = SEQUENCE_BIT;
        private static final long DC_SHIFT = WORKER_SHIFT + WORKER_BIT;
        private static final long TIMESTAMP_SHIFT = DC_SHIFT + DC_BIT;

        public static long BASE_TIME = (new Date(117, Calendar.NOVEMBER, 28)).getTime();

        public IdWorker(long workerId, long datacenterId) {
            // Sanity check for workerId
            if (workerId > MAX_WORKER_ID || workerId < 0) {
                throw new IllegalArgumentException(String.format(
                        "Worker id can't > %d or < 0",
                        MAX_WORKER_ID));
            }
            if (datacenterId > MAX_DC_ID || datacenterId < 0) {
                throw new IllegalArgumentException(String.format(
                        "Datacenter id can't > %d or < 0",
                        MAX_DC_ID));
            }
            this.workerId = workerId;
            this.datacenterId = datacenterId;
        }

        public synchronized long nextId() {
            long timestamp = timeGen();

            if (timestamp > this.lastTimestamp) {
                this.sequence = 0L;
            } else if (timestamp == this.lastTimestamp) {
                this.sequence = (this.sequence + 1) & SEQUENCE_MASK;
                if (this.sequence == 0) {
                    timestamp = tillNextMillis(this.lastTimestamp);
                }
            } else {
                assert timestamp < this.lastTimestamp;
                throw new BcException(
                        String.format("Clock moved backwards. Refusing to generate id for %d milliseconds",
                                this.lastTimestamp - timestamp)
                );
            }

            this.lastTimestamp = timestamp;

            return (timestamp << TIMESTAMP_SHIFT) |
                    (this.datacenterId << DC_SHIFT) |
                    (this.workerId << WORKER_SHIFT) |
                    (this.sequence);
        }

        private long timeGen() {
            return System.currentTimeMillis() - BASE_TIME;
        }

        private long tillNextMillis(long lastTimestamp) {
            long timestamp;
            for (timestamp = timeGen(); timestamp <= lastTimestamp; timestamp = timeGen()) {
            }

            return timestamp;
        }
    }
}
