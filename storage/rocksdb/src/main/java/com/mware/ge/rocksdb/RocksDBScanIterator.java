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
package com.mware.ge.rocksdb;

import com.mware.ge.GeException;
import com.mware.ge.collection.Pair;
import com.mware.ge.store.kv.ScanIterator;
import com.mware.ge.util.Bytes;
import com.mware.ge.util.Preconditions;
import org.rocksdb.RocksIterator;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * A wrapper for RocksIterator that convert RocksDB results to std Iterator
 */
public class RocksDBScanIterator implements ScanIterator {
    private final RocksIterator iter;
    private final byte[] keyBegin;
    private final byte[] keyEnd;
    private final int scanType;

    private byte[] position;
    private boolean matched;

    public RocksDBScanIterator(RocksIterator iter, byte[] keyBegin, byte[] keyEnd, int scanType) {
        Preconditions.checkNotNull(iter, "iter");
        this.iter = iter;
        this.keyBegin = keyBegin;
        this.keyEnd = keyEnd;
        this.scanType = scanType;

        this.position = keyBegin;
        this.matched = false;

        this.checkArguments();

        //this.dump();

        this.seek();
    }

    @Override
    public boolean hasNext() {
        this.matched = this.iter.isOwningHandle();
        if (!this.matched) {
            // Maybe closed
            return this.matched;
        }

        this.matched = this.iter.isValid();
        if (this.matched) {
            // Update position for paging
            this.position = this.iter.key();
            // Do filter if not SCAN_ANY
            if (!this.match(SCAN_ANY)) {
                this.matched = this.filter(this.position);
            }
        }
        if (!this.matched) {
            // The end
            this.position = null;
            // Free the iterator if finished
            this.close();
        }
        return this.matched;
    }

    @Override
    public Pair<byte[], byte[]> next() {
        if (!this.matched) {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
        }

        Pair<byte[], byte[]> col = Pair.of(this.iter.key(), this.iter.value());
        this.iter.next();
        this.matched = false;

        return col;
    }

    public long count() {
        long count = 0L;
        while (this.hasNext()) {
            this.iter.next();
            this.matched = false;
            count++;
            if (Thread.interrupted()) {
                throw new GeException("Interrupted, maybe it is timed out",
                        new InterruptedException());
            }
        }
        return count;
    }

    public byte[] position() {
        return this.position;
    }

    @Override
    public void close() {
        if (this.iter.isOwningHandle()) {
            this.iter.close();
        }
    }

    private boolean filter(byte[] key) {
        if (this.match(SCAN_PREFIX_BEGIN)) {
            /*
             * Prefix with `keyBegin`?
             * TODO: use custom prefix_extractor instead
             *       or use ReadOptions.prefix_same_as_start
             */
            return Bytes.prefixWith(key, this.keyBegin);
        } else if (this.match(SCAN_PREFIX_END)) {
            /*
             * Prefix with `keyEnd`?
             * like the following query for range index:
             *  key > 'age:20' and prefix with 'age'
             */
            assert this.keyEnd != null;
            return Bytes.prefixWith(key, this.keyEnd);
        } else if (this.match(SCAN_LT_END)) {
            /*
             * Less (equal) than `keyEnd`?
             * NOTE: don't use BytewiseComparator due to signed byte
             */
            assert this.keyEnd != null;
            if (this.match(SCAN_LTE_END)) {
                // Just compare the prefix, maybe there are excess tail
                key = Arrays.copyOfRange(key, 0, this.keyEnd.length);
                return Bytes.compare(key, this.keyEnd) <= 0;
            } else {
                return Bytes.compare(key, this.keyEnd) < 0;
            }
        } else {
            assert this.match(SCAN_ANY) ||
                    this.match(SCAN_GT_BEGIN) ||
                    this.match(SCAN_GTE_BEGIN) :
                    "Unknow scan type";
            return true;
        }
    }

    private void seek() {
        if (this.keyBegin == null) {
            // Seek to the first if no `keyBegin`
            this.iter.seekToFirst();
        } else {
            /*
             * Seek to `keyBegin`:
             * if set SCAN_GT_BEGIN/SCAN_GTE_BEGIN (key > / >= 'xx')
             * or if set SCAN_PREFIX_WITH_BEGIN (key prefix with 'xx')
             */
            this.iter.seek(this.keyBegin);

            // Skip `keyBegin` if set SCAN_GT_BEGIN (key > 'xx')
            if (this.match(SCAN_GT_BEGIN) &&
                    !this.match(SCAN_GTE_BEGIN)) {
                while (this.iter.isValid() &&
                        Bytes.equals(this.iter.key(), this.keyBegin)) {
                    this.iter.next();
                }
            }
        }
    }

    private void checkArguments() {
        Preconditions.checkArgument(!(this.match(SCAN_PREFIX_BEGIN) &&
                        this.match(SCAN_PREFIX_END)),
                "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                        "SCAN_PREFIX_WITH_END at the same time");

        Preconditions.checkArgument(!(this.match(SCAN_PREFIX_BEGIN) &&
                        this.match(SCAN_GT_BEGIN)),
                "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                        "SCAN_GT_BEGIN/SCAN_GTE_BEGIN at the same time");

        Preconditions.checkArgument(!(this.match(SCAN_PREFIX_END) &&
                        this.match(SCAN_LT_END)),
                "Can't set SCAN_PREFIX_WITH_END and " +
                        "SCAN_LT_END/SCAN_LTE_END at the same time");

        if (this.match(SCAN_PREFIX_BEGIN)) {
            Preconditions.checkArgument(this.keyBegin != null,
                    "Parameter `keyBegin` can't be null " +
                            "if set SCAN_PREFIX_WITH_BEGIN");
            Preconditions.checkArgument(this.keyEnd == null,
                    "Parameter `keyEnd` must be null " +
                            "if set SCAN_PREFIX_WITH_BEGIN");
        }

        if (this.match(SCAN_PREFIX_END)) {
            Preconditions.checkArgument(this.keyEnd != null,
                    "Parameter `keyEnd` can't be null " +
                            "if set SCAN_PREFIX_WITH_END");
        }

        if (this.match(SCAN_GT_BEGIN)) {
            Preconditions.checkArgument(this.keyBegin != null,
                    "Parameter `keyBegin` can't be null " +
                            "if set SCAN_GT_BEGIN or SCAN_GTE_BEGIN");
        }

        if (this.match(SCAN_LT_END)) {
            Preconditions.checkArgument(this.keyEnd != null,
                    "Parameter `keyEnd` can't be null " +
                            "if set SCAN_LT_END or SCAN_LTE_END");
        }
    }

    private boolean match(int expected) {
        return matchScanType(expected, this.scanType);
    }

    public static boolean matchScanType(int expected, int actual) {
        return (expected & actual) == expected;
    }
}
