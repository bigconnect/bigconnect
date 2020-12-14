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
package com.mware.ge.store.util;

import com.mware.ge.FetchHints;
import com.mware.ge.GeException;
import com.mware.ge.Metadata;
import com.mware.ge.Visibility;
import com.mware.ge.id.NameSubstitutionStrategy;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.values.storable.Value;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LazyPropertyMetadata implements Metadata {
    private static final String KEY_SEPARATOR = "\u001f";
    private transient ReadWriteLock entriesLock = new ReentrantReadWriteLock();
    private final Map<String, Metadata.Entry> entries = new ConcurrentHashMap<>();
    private final List<MetadataEntry> metadataEntries;
    private int[] metadataIndexes;
    private final Set<String> removedEntries = new HashSet<>();
    private final GeSerializer geSerializer;
    private final NameSubstitutionStrategy nameSubstitutionStrategy;
    private final FetchHints fetchHints;

    public LazyPropertyMetadata(
            List<MetadataEntry> metadataEntries,
            int[] metadataIndexes,
            GeSerializer geSerializer,
            NameSubstitutionStrategy nameSubstitutionStrategy,
            FetchHints fetchHints
    ) {
        this.metadataEntries = metadataEntries;
        this.metadataIndexes = metadataIndexes;
        this.geSerializer = geSerializer;
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
        this.fetchHints = fetchHints;
    }

    @Override
    public Metadata add(String key, Value value, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            String mapKey = toMapKey(key, visibility);
            removedEntries.remove(mapKey);
            entries.put(mapKey, new Entry(key, value, visibility));
            return this;
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void remove(String key, Visibility visibility) {
        getEntriesLock().writeLock().lock();
        try {
            String mapKey = toMapKey(key, visibility);
            removedEntries.add(mapKey);
            entries.remove(mapKey);
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void remove(String key) {
        getEntriesLock().writeLock().lock();
        try {
            if (metadataIndexes != null && metadataEntries != null) {
                for (int metadataIndex : metadataIndexes) {
                    MetadataEntry entry = metadataEntries.get(metadataIndex);
                    String metadataKey = entry.getMetadataKey(nameSubstitutionStrategy);
                    if (metadataKey.equals(key)) {
                        Visibility metadataVisibility = entry.getVisibility();
                        String mapKey = toMapKey(metadataKey, metadataVisibility);
                        removedEntries.add(mapKey);
                    }
                }
            }
            for (Map.Entry<String, Metadata.Entry> e : new ArrayList<>(entries.entrySet())) {
                if (e.getValue().getKey().equals(key)) {
                    entries.remove(e.getKey());
                }
            }
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        getEntriesLock().writeLock().lock();
        try {
            entries.clear();
            if (metadataEntries != null) {
                metadataEntries.clear();
            }
            metadataIndexes = new int[0];
            removedEntries.clear();
        } finally {
            getEntriesLock().writeLock().unlock();
        }
    }

    private void loadAll() {
        if (metadataEntries != null && metadataIndexes != null) {
            for (int metadataIndex : metadataIndexes) {
                MetadataEntry metadataEntry = metadataEntries.get(metadataIndex);
                String metadataKey = metadataEntry.getMetadataKey(nameSubstitutionStrategy);
                Visibility metadataVisibility = metadataEntry.getVisibility();
                String mapKey = toMapKey(metadataKey, metadataVisibility);
                if (removedEntries.contains(mapKey)) {
                    continue;
                }
                if (entries.containsKey(mapKey)) {
                    continue;
                }
                LazyEntry lazyEntry = new LazyEntry(metadataKey, metadataVisibility, metadataEntry);
                entries.put(mapKey, lazyEntry);
            }
        }
    }

    @Override
    public Collection<Metadata.Entry> entrySet() {
        getEntriesLock().readLock().lock();
        try {
            loadAll();
            return new ArrayList<>(entries.values());
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Metadata.Entry getEntry(String key, Visibility visibility) {
        String mapKey = toMapKey(key, visibility);

        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();

        if (removedEntries.contains(mapKey)) {
            return null;
        }

        try {
            Metadata.Entry entry = entries.get(mapKey);
            if (entry != null) {
                return entry;
            }

            if (metadataEntries != null && metadataIndexes != null) {
                for (int metadataIndex : metadataIndexes) {
                    MetadataEntry metadataEntry = metadataEntries.get(metadataIndex);
                    String metadataKey = metadataEntry.getMetadataKey(nameSubstitutionStrategy);
                    if (metadataKey.equals(key)) {
                        Visibility metadataVisibility = metadataEntry.getVisibility();
                        if (metadataVisibility.equals(visibility)) {
                            LazyEntry lazyEntry = new LazyEntry(metadataKey, metadataVisibility, metadataEntry);
                            entries.put(mapKey, lazyEntry);
                            return lazyEntry;
                        }
                    }
                }
            }

            return null;
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public Metadata.Entry getEntry(String key) {
        getFetchHints().assertMetadataIncluded(key);
        getEntriesLock().readLock().lock();
        try {
            Metadata.Entry entry = null;

            for (Map.Entry<String, Metadata.Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    if (entry != null) {
                        throw new GeException("Multiple matching entries for key: " + key);
                    }
                    entry = e.getValue();
                }
            }

            if (metadataEntries != null && metadataIndexes != null) {
                for (int metadataIndex : metadataIndexes) {
                    MetadataEntry metadataEntry = metadataEntries.get(metadataIndex);
                    String metadataKey = metadataEntry.getMetadataKey(nameSubstitutionStrategy);
                    if (metadataKey.equals(key)) {
                        Visibility metadataVisibility = metadataEntry.getVisibility();
                        String mapKey = toMapKey(metadataKey, metadataVisibility);
                        if (entries.containsKey(mapKey)) {
                            continue;
                        }
                        if (entry != null) {
                            throw new GeException("Multiple matching entries for key: " + key);
                        }
                        entry = new LazyEntry(metadataKey, metadataVisibility, metadataEntry);
                        entries.put(mapKey, entry);
                    }
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
            Map<String, Metadata.Entry> results = new HashMap<>();

            for (Map.Entry<String, Metadata.Entry> e : entries.entrySet()) {
                if (e.getValue().getKey().equals(key)) {
                    String mapKey = toMapKey(e.getValue().getKey(), e.getValue().getVisibility());
                    results.put(mapKey, e.getValue());
                }
            }

            if (metadataEntries != null && metadataIndexes != null) {
                for (int metadataIndex : metadataIndexes) {
                    MetadataEntry metadataEntry = metadataEntries.get(metadataIndex);
                    String metadataKey = metadataEntry.getMetadataKey(nameSubstitutionStrategy);
                    if (metadataKey.equals(key)) {
                        Visibility metadataVisibility = metadataEntry.getVisibility();
                        String mapKey = toMapKey(metadataKey, metadataVisibility);
                        if (!results.containsKey(mapKey)) {
                            LazyEntry entry = new LazyEntry(metadataKey, metadataVisibility, metadataEntry);
                            entries.put(mapKey, entry);
                            results.put(mapKey, entry);
                        }
                    }
                }
            }

            return results.values();
        } finally {
            getEntriesLock().readLock().unlock();
        }
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    private ReadWriteLock getEntriesLock() {
        // entriesLock may be null if this class has just been de-serialized
        if (entriesLock == null) {
            entriesLock = new ReentrantReadWriteLock();
        }
        return entriesLock;
    }

    public static String toMapKey(String key, Visibility visibility) {
        return key + KEY_SEPARATOR + visibility.getVisibilityString();
    }

    private static class Entry implements Metadata.Entry {
        private final String key;
        private final Value value;
        private final Visibility visibility;

        public Entry(String key, Value value, Visibility visibility) {
            this.key = key;
            this.value = value;
            this.visibility = visibility;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Value getValue() {
            return value;
        }

        @Override
        public Visibility getVisibility() {
            return visibility;
        }
    }

    private class LazyEntry implements Metadata.Entry {
        private final String key;
        private final Visibility visibility;
        private final MetadataEntry metadataEntry;
        private Value metadataValue;

        public LazyEntry(String key, Visibility visibility, MetadataEntry metadataEntry) {
            this.key = key;
            this.visibility = visibility;
            this.metadataEntry = metadataEntry;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Value getValue() {
            if (metadataValue == null) {
                metadataValue = metadataEntry.getValue(geSerializer);
                if (metadataValue == null) {
                    throw new GeException("Invalid metadata value found.");
                }
            }
            return metadataValue;
        }

        @Override
        public Visibility getVisibility() {
            return visibility;
        }
    }
}
