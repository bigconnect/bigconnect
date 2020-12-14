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

import com.mware.ge.*;
import com.mware.ge.property.MutableProperty;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.values.storable.StreamingPropertyValueRef;
import com.mware.ge.values.storable.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class LazyMutableProperty extends MutableProperty {
    private final Graph graph;
    private final GeSerializer geSerializer;
    private final String propertyKey;
    private final String propertyName;
    private Long timestamp;
    private final FetchHints fetchHints;
    private Set<Visibility> hiddenVisibilities;
    private byte[] propertyValue;
    private MetadataRef metadataRef;
    private Visibility visibility;
    private transient Value cachedPropertyValue;
    private transient Metadata cachedMetadata;

    public LazyMutableProperty(
            Graph graph,
            GeSerializer geSerializer,
            String propertyKey,
            String propertyName,
            byte[] propertyValue,
            MetadataRef metadataRef,
            Set<Visibility> hiddenVisibilities,
            Visibility visibility,
            long timestamp,
            FetchHints fetchHints
    ) {
        this.graph = graph;
        this.geSerializer = geSerializer;
        this.propertyKey = propertyKey;
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
        this.metadataRef = metadataRef;
        this.visibility = visibility;
        this.hiddenVisibilities = hiddenVisibilities;
        this.timestamp = timestamp;
        this.fetchHints = fetchHints;
    }

    @Override
    public synchronized void setValue(Value value) {
        this.cachedPropertyValue = value;
        this.propertyValue = null;
    }

    @Override
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void setVisibility(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public void addHiddenVisibility(Visibility visibility) {
        if (hiddenVisibilities == null) {
            hiddenVisibilities = new HashSet<>();
        }
        hiddenVisibilities.add(visibility);
    }

    @Override
    public void removeHiddenVisibility(Visibility visibility) {
        if (hiddenVisibilities == null) {
            hiddenVisibilities = new HashSet<>();
        }
        hiddenVisibilities.remove(visibility);
    }

    @Override
    protected void updateMetadata(com.mware.ge.Property property) {
        this.cachedMetadata = null;
        if (property instanceof LazyMutableProperty) {
            this.metadataRef = ((LazyMutableProperty) property).metadataRef;
        } else {
            Collection<Metadata.Entry> entries = new ArrayList<>(property.getMetadata().entrySet());
            this.metadataRef = null;
            if (getFetchHints().isIncludePropertyAndMetadata(propertyName)) {
                for (Metadata.Entry metadataEntry : entries) {
                    getMetadata().add(metadataEntry.getKey(), metadataEntry.getValue(), metadataEntry.getVisibility());
                }
            }
        }
    }

    @Override
    public String getKey() {
        return this.propertyKey;
    }

    @Override
    public String getName() {
        return this.propertyName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Value getValue() {
        if (cachedPropertyValue == null) {
            synchronized(this) {
                if (cachedPropertyValue == null) {
                    if (propertyValue == null || propertyValue.length == 0) {
                        return null;
                    }
                    cachedPropertyValue = this.geSerializer.bytesToObject(propertyValue);
                    propertyValue = null;
                    if (cachedPropertyValue instanceof StreamingPropertyValueRef) {
                        cachedPropertyValue = ((StreamingPropertyValueRef) cachedPropertyValue).toStreamingPropertyValue(this.graph, getTimestamp());
                    }
                }
            }
        }
        return cachedPropertyValue;
    }

    @Override
    public Visibility getVisibility() {
        return this.visibility;
    }

    @Override
    public Metadata getMetadata() {
        if (!fetchHints.isIncludePropertyMetadata()) {
            throw new GeMissingFetchHintException(fetchHints, "includePropertyMetadata");
        }
        if (cachedMetadata == null) {
            if (metadataRef == null) {
                cachedMetadata = Metadata.create(fetchHints);
            } else {
                cachedMetadata = new LazyPropertyMetadata(
                        metadataRef.getMetadataEntries(),
                        metadataRef.getMetadataIndexes(),
                        this.geSerializer,
                        graph.getNameSubstitutionStrategy(),
                        fetchHints
                );
                metadataRef = null;
            }
        }
        return cachedMetadata;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        if (hiddenVisibilities == null) {
            return new ArrayList<>();
        }
        return hiddenVisibilities;
    }

    @Override
    public boolean isHidden(Authorizations authorizations) {
        if (hiddenVisibilities != null) {
            for (Visibility v : getHiddenVisibilities()) {
                if (authorizations.canRead(v)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }
}
