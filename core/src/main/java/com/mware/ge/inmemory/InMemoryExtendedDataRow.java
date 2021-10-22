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

import com.google.common.annotations.VisibleForTesting;
import com.mware.ge.*;
import com.mware.ge.security.ColumnVisibility;
import com.mware.ge.security.VisibilityEvaluator;
import com.mware.ge.security.VisibilityParseException;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.values.storable.Value;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class InMemoryExtendedDataRow extends ExtendedDataRowBase {
    private final ExtendedDataRowId id;
    private ReadWriteLock propertiesLock = new ReentrantReadWriteLock();
    private Set<InMemoryProperty> properties = new HashSet<>();

    public InMemoryExtendedDataRow(ExtendedDataRowId id, FetchHints fetchHints) {
        super(fetchHints);
        this.id = id;
    }

    public boolean canRead(VisibilityEvaluator visibilityEvaluator, FetchHints fetchHints) {
        propertiesLock.readLock().lock();
        try {
            return properties.stream().anyMatch(e -> e.canRead(visibilityEvaluator));
        } finally {
            propertiesLock.readLock().unlock();
        }
    }

    @Override
    public ExtendedDataRowId getId() {
        return id;
    }

    public InMemoryExtendedDataRow toReadable(VisibilityEvaluator visibilityEvaluator, FetchHints fetchHints) {
        propertiesLock.readLock().lock();
        try {
            InMemoryExtendedDataRow row = new InMemoryExtendedDataRow(getId(), getFetchHints());
            for (InMemoryProperty column : properties) {
                if (column.canRead(visibilityEvaluator)) {
                    row.properties.add(column);
                }
            }
            return row;
        } finally {
            propertiesLock.readLock().unlock();
        }
    }

    public void addColumn(
            String propertyName,
            String key,
            Value value,
            long timestamp,
            Visibility visibility
    ) {
        propertiesLock.writeLock().lock();
        try {
            InMemoryProperty prop = new InMemoryProperty(propertyName, key, value, FetchHints.ALL, timestamp, visibility);
            properties.remove(prop);
            properties.add(prop);
        } finally {
            propertiesLock.writeLock().unlock();
        }
    }

    public void removeColumn(String columnName, String key, Visibility visibility) {
        propertiesLock.writeLock().lock();
        try {
            properties.removeIf(p ->
                    p.getName().equals(columnName)
                            && p.getVisibility().equals(visibility)
                            && ((key == null && p.getKey() == null) || (key != null && key.equals(p.getKey())))
            );
        } finally {
            propertiesLock.writeLock().unlock();
        }
    }

    @Override
    public Iterable<Property> getProperties() {
        propertiesLock.readLock().lock();
        try {
            return this.properties.stream().map(p -> (Property) p).collect(Collectors.toList());
        } finally {
            propertiesLock.readLock().unlock();
        }
    }

    @VisibleForTesting
    public static class InMemoryProperty extends Property {
        private final String name;
        private final String key;
        private final Long timestamp;
        private final Value value;
        private final Visibility visibility;
        private final ColumnVisibility columnVisibility;
        private final FetchHints fetchHints;

        public InMemoryProperty(
                String name,
                String key,
                Value value,
                FetchHints fetchHints,
                long timestamp,
                Visibility visibility
        ) {
            this.name = name;
            this.key = key;
            this.value = value;
            this.fetchHints = fetchHints;
            this.timestamp = timestamp;
            this.visibility = visibility;
            this.columnVisibility = new ColumnVisibility(visibility.getVisibilityString());
        }

        public boolean canRead(VisibilityEvaluator visibilityEvaluator) {
            try {
                return visibilityEvaluator.evaluate(columnVisibility);
            } catch (VisibilityParseException e) {
                throw new GeException("could not evaluate visibility " + visibility.getVisibilityString(), e);
            }
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getName() {
            return name;
        }

        public Value getValue() {
            return value;
        }

        @Override
        public Long getTimestamp() {
            return timestamp;
        }

        @Override
        public FetchHints getFetchHints() {
            return fetchHints;
        }

        @Override
        public Visibility getVisibility() {
            return visibility;
        }

        @Override
        public Metadata getMetadata() {
            return Metadata.create(getFetchHints());
        }

        @Override
        public Iterable<Visibility> getHiddenVisibilities() {
            return new ArrayList<>();
        }

        @Override
        public boolean isHidden(Authorizations authorizations) {
            return false;
        }
    }
}
