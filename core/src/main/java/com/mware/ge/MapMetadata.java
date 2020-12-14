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
package com.mware.ge;

import com.mware.ge.values.storable.Value;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MapMetadata implements Metadata, Serializable {
    private static final String KEY_SEPARATOR = "\u001f";
    private final Map<String, Entry> entries;
    private final FetchHints fetchHints;
    private transient ReadWriteLock entriesLock = new ReentrantReadWriteLock();

    public MapMetadata() {
        this(FetchHints.ALL);
    }

    public MapMetadata(FetchHints fetchHints) {
        this.entries = new HashMap<>();
        this.fetchHints = fetchHints;
    }

    public MapMetadata(Metadata copyFromMetadata) {
        this(copyFromMetadata, FetchHints.ALL);
    }

    public MapMetadata(Metadata copyFromMetadata, FetchHints fetchHints) {
        this(fetchHints);
        if (copyFromMetadata != null) {
            for (Metadata.Entry entry : copyFromMetadata.entrySet()) {
                add(entry.getKey(), entry.getValue(), entry.getVisibility());
            }
        }
    }

    @Override
    public Metadata add(String key, Value value, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            entries.put(toMapKey(key, visibility), new Entry(key, value, visibility));
            return this;
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void remove(String key, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            entries.remove(toMapKey(key, visibility));
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        getEntriesLock().writeLock().lock();
        try {
            entries.clear();
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void remove(String key) {
        getEntriesLock().writeLock().lock();
        try {
            for (Map.Entry<String, Entry> e : new ArrayList<>(entries.entrySet())) {
                if (e.getValue().getKey().equals(key)) {
                    entries.remove(e.getKey());
                }
            }
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public Collection<Metadata.Entry> entrySet() {
        getEntriesLock().readLock().lock();
        try {
            return new ArrayList<>(entries.values());
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Entry getEntry(String key, Visibility visibility) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            return entries.get(toMapKey(key, visibility));
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Entry getEntry(String key) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            Entry entry = null;
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    if (entry != null) {
                        throw new GeException("Multiple matching entries for key: " + key);
                    }
                    entry = e.getValue();
                }
            }
            return entry;
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Collection<Metadata.Entry> getEntries(String key) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            Collection<Metadata.Entry> results = new ArrayList<>();
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    Entry entry = e.getValue();
                    results.add(entry);
                }
            }
            return results;
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(String key) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    return true;
                }
            }
            return false;
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    private String toMapKey(String key, Visibility visibility) {
        return key + KEY_SEPARATOR + visibility.getVisibilityString();
    }

    private ReadWriteLock getEntriesLock() {
        // entriesLock may be null if this class has just been deserialized
        if (entriesLock == null) {
            entriesLock = new ReentrantReadWriteLock();
        }
        return entriesLock;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof MapMetadata))
            return false;

        MapMetadata other = (MapMetadata) obj;

        return Objects.equals(entries, other.entries);
    }

    class Entry implements Metadata.Entry, Serializable {
        static final long serialVersionUID = 42L;
        private final String key;
        private final Value value;
        private final Visibility visibility;

        protected Entry(String key, Value value, Visibility visibility) {
            this.key = key;
            this.value = value;
            this.visibility = visibility;
        }

        public String getKey() {
            return key;
        }

        public Value getValue() {
            return value;
        }

        public Visibility getVisibility() {
            return visibility;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "key='" + key + '\'' +
                    ", value=" + value +
                    ", visibility=" + visibility +
                    '}';
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof Entry))
                return false;

            Entry other = (Entry) obj;

            return Objects.equals(key, other.key)
                    && Objects.equals(value, other.value)
                    && Objects.equals(visibility, other.visibility);
        }
    }
}
