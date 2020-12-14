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
package com.mware.ge.store.mutations;

import java.util.Arrays;

/**
 * A single column and value pair within a {@link StoreMutation}.
 *
 */
public class StoreColumnUpdate {

    private byte[] columnFamily;
    private byte[] columnQualifier;
    private byte[] columnVisibility;
    private long timestamp;
    private boolean hasTimestamp;
    private byte[] val;
    private boolean deleted;

    /**
     * Creates a new column update.
     *
     * @param cf
     *          column family
     * @param cq
     *          column qualifier
     * @param cv
     *          column visibility
     * @param hasts
     *          true if the update specifies a timestamp
     * @param ts
     *          timestamp
     * @param deleted
     *          delete marker
     * @param val
     *          cell value
     */
    public StoreColumnUpdate(byte[] cf, byte[] cq, byte[] cv, boolean hasts, long ts, boolean deleted,
                        byte[] val) {
        this.columnFamily = cf;
        this.columnQualifier = cq;
        this.columnVisibility = cv;
        this.hasTimestamp = hasts;
        this.timestamp = ts;
        this.deleted = deleted;
        this.val = val;
    }

    /**
     * Gets whether this update specifies a timestamp.
     *
     * @return true if this update specifies a timestamp
     */
    public boolean hasTimestamp() {
        return hasTimestamp;
    }

    /**
     * Gets the column family for this update. Not a defensive copy.
     *
     * @return column family
     */
    public byte[] getColumnFamily() {
        return columnFamily;
    }

    /**
     * Gets the column qualifier for this update. Not a defensive copy.
     *
     * @return column qualifier
     */
    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    /**
     * Gets the column visibility for this update.
     *
     * @return column visibility
     */
    public byte[] getColumnVisibility() {
        return columnVisibility;
    }

    /**
     * Gets the timestamp for this update.
     *
     * @return timestamp
     */
    public long getTimestamp() {
        return this.timestamp;
    }

    /**
     * Gets the delete marker for this update.
     *
     * @return delete marker
     */
    public boolean isDeleted() {
        return this.deleted;
    }

    /**
     * Gets the cell value for this update.
     *
     * @return cell value
     */
    public byte[] getValue() {
        return this.val;
    }

    @Override
    public String toString() {
        return new String(columnFamily) + ":" + new String(columnQualifier) + " ["
                + new String(columnVisibility) + "] " + (hasTimestamp ? timestamp : "NO_TIME_STAMP")
                + " " + new String(val) + " " + deleted;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StoreColumnUpdate))
            return false;
        StoreColumnUpdate upd = (StoreColumnUpdate) obj;
        return Arrays.equals(getColumnFamily(), upd.getColumnFamily())
                && Arrays.equals(getColumnQualifier(), upd.getColumnQualifier())
                && Arrays.equals(getColumnVisibility(), upd.getColumnVisibility())
                && isDeleted() == upd.isDeleted() && Arrays.equals(getValue(), upd.getValue())
                && hasTimestamp() == upd.hasTimestamp() && getTimestamp() == upd.getTimestamp();
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(columnFamily) + Arrays.hashCode(columnQualifier)
                + Arrays.hashCode(columnVisibility)
                + (hasTimestamp ? (Boolean.TRUE.hashCode() + Long.valueOf(timestamp).hashCode())
                : Boolean.FALSE.hashCode())
                + (deleted ? Boolean.TRUE.hashCode() : (Boolean.FALSE.hashCode() + Arrays.hashCode(val)));
    }
}

