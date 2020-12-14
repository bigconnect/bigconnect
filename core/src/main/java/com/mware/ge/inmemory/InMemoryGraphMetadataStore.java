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
package com.mware.ge.inmemory;

import com.mware.ge.GraphMetadataEntry;
import com.mware.ge.GraphMetadataStore;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.JavaSerializableUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class InMemoryGraphMetadataStore extends GraphMetadataStore implements Serializable {
    private final ReadWriteLock metadataLock = new ReentrantReadWriteLock();
    private final Map<String, byte[]> metadata = new HashMap<>();

    @Override
    public Iterable<GraphMetadataEntry> getMetadata() {
        metadataLock.readLock().lock();
        try {
            return this.metadata.entrySet().stream()
                    .map(o -> new GraphMetadataEntry(o.getKey(), o.getValue()))
                    .collect(Collectors.toList());
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public void reloadMetadata() {
        // Nothing to do here
    }

    @Override
    public Object getMetadata(String key) {
        metadataLock.readLock().lock();
        try {
            byte[] bytes = this.metadata.get(key);
            if (bytes == null) {
                return null;
            }
            return JavaSerializableUtils.bytesToObject(bytes);
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public void setMetadata(String key, Object value) {
        metadataLock.writeLock().lock();
        try {
            this.metadata.put(key, JavaSerializableUtils.objectToBytes(value));
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public void removeMetadata(String key) {
        metadataLock.writeLock().lock();
        try {
            this.metadata.remove(key);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void drop() {

    }
}
