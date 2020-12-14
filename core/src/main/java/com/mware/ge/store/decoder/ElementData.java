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
package com.mware.ge.store.decoder;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.mware.ge.FetchHints;
import com.mware.ge.GeException;
import com.mware.ge.Property;
import com.mware.ge.Visibility;
import com.mware.ge.security.ByteSequence;
import com.mware.ge.store.StorableGraph;
import com.mware.ge.store.util.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ElementData {
    public String id;
    public long timestamp;
    public String visibility;
    public StorableGraph graph;
    public final List<String> hiddenVisibilities = new ArrayList<>();
    public long softDeleteTimestamp;
    public final List<SoftDeletedProperty> softDeletedProperties = new ArrayList<>();
    public final List<HiddenProperty> hiddenProperties = new ArrayList<>();
    public final List<DecoderMetadataEntry> metadataEntries = new ArrayList<>();
    public final Map<ByteSequence, List<Integer>> propertyMetadata = new HashMap<>();
    public final Map<ByteSequence, PropertyColumnQualifierByteSequence> propertyColumnQualifiers = new HashMap<>();
    public final Map<ByteSequence, byte[]> propertyValues = new HashMap<>();
    public final Map<ByteSequence, ByteSequence> propertyVisibilities = new HashMap<>();
    public final Map<ByteSequence, Long> propertyTimestamps = new HashMap<>();
    public final Set<String> extendedTableNames = new HashSet<>();

    private Iterable<PropertyRef> internalGetProperties(FetchHints fetchHints) {
        final List<PropertyRef> results = new ArrayList<>();
        try {
            iterateProperties((
                    propertyKey,
                    propertyName,
                    propertyValue,
                    propertyVisibility,
                    propertyTimestamp,
                    propertyHiddenVisibilities,
                    metadata
            ) -> results.add(new PropertyRef(
                    propertyKey,
                    propertyName,
                    propertyValue,
                    propertyVisibility,
                    propertyTimestamp,
                    propertyHiddenVisibilities,
                    metadata
            )), fetchHints);
        } catch (IOException ex) {
            throw new GeException("Could not get properties", ex);
        }
        return results;
    }

    public Iterable<Property> getProperties(FetchHints fetchHints) {
        return Iterables.transform(internalGetProperties(fetchHints), new Function<PropertyRef, Property>() {
            @Nullable
            @Override
            public Property apply(@Nullable PropertyRef property) {
                Set<Visibility> hiddenVisibilities = null;
                if (property.hiddenVisibilities != null) {
                    hiddenVisibilities = Sets.newHashSet(Iterables.transform(property.hiddenVisibilities, new Function<ByteSequence, Visibility>() {
                        @Nullable
                        @Override
                        public Visibility apply(ByteSequence visibilityText) {
                            return new Visibility(visibilityText.toString());
                        }
                    }));
                }
                Visibility visibility = new Visibility(property.visibility.toString());
                MetadataRef ref = null;

                if (property.metadata != null) {
                    int[] metadataIndexes = new int[property.metadata.size()];
                    for (int i = 0; i < property.metadata.size(); i++) {
                        metadataIndexes[i] = property.metadata.get(i);
                    }

                    ref = new MetadataRef(
                            metadataEntries.parallelStream().map(
                                    dme -> new MetadataEntry(dme.metadataKey.toString(), dme.metadataVisibility.toString(), dme.value)
                            ).collect(Collectors.toList()),
                            metadataIndexes
                    );
                }

                return new LazyMutableProperty(
                        graph,
                        graph.getGeSerializer(),
                        graph.getNameSubstitutionStrategy().inflate(new String(property.key.toArray())),
                        graph.getNameSubstitutionStrategy().inflate(new String(property.name.toArray())),
                        property.value,
                        ref,
                        hiddenVisibilities,
                        visibility,
                        property.timestamp,
                        FetchHints.ALL_INCLUDING_HIDDEN
                );
            }
        });
    }

    private void iterateProperties(PropertyDataHandler propertyDataHandler, FetchHints fetchHints) throws IOException {
        boolean includeHidden = fetchHints.isIncludeHidden();
        for (Map.Entry<ByteSequence, byte[]> propertyValueEntry : propertyValues.entrySet()) {
            ByteSequence key = propertyValueEntry.getKey();
            PropertyColumnQualifierByteSequence propertyColumnQualifier = propertyColumnQualifiers.get(key);
            ByteSequence propertyKey = propertyColumnQualifier.getPropertyKey();
            ByteSequence propertyName = propertyColumnQualifier.getPropertyName();
            byte[] propertyValue = propertyValueEntry.getValue();
            ByteSequence propertyVisibility = propertyVisibilities.get(key);
            long propertyTimestamp = propertyTimestamps.get(key);
            if (propertyTimestamp < softDeleteTimestamp) {
                continue;
            }
            Set<ByteSequence> propertyHiddenVisibilities = getPropertyHiddenVisibilities(propertyKey, propertyName, propertyVisibility);
            if (!includeHidden && isHidden(propertyKey, propertyName, propertyVisibility)) {
                continue;
            }
            if (isPropertyDeleted(propertyKey, propertyName, propertyTimestamp, propertyVisibility)) {
                continue;
            }
            List<Integer> metadata = propertyMetadata.get(key);
            propertyDataHandler.handle(
                    propertyKey,
                    propertyName,
                    propertyValue,
                    propertyVisibility,
                    propertyTimestamp,
                    propertyHiddenVisibilities,
                    metadata
            );
        }
    }

    private interface PropertyDataHandler {
        void handle(
                ByteSequence propertyKey,
                ByteSequence propertyName,
                byte[] propertyValue,
                ByteSequence propertyVisibility,
                long propertyTimestamp,
                Set<ByteSequence> propertyHiddenVisibilities,
                List<Integer> metadata
        ) throws IOException;
    }

    private Set<ByteSequence> getPropertyHiddenVisibilities(
            ByteSequence propertyKey,
            ByteSequence propertyName,
            ByteSequence propertyVisibility
    ) {
        Set<ByteSequence> hiddenVisibilities = null;
        for (HiddenProperty hiddenProperty : hiddenProperties) {
            if (hiddenProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                if (hiddenVisibilities == null) {
                    hiddenVisibilities = new HashSet<>();
                }
                hiddenVisibilities.add(hiddenProperty.getHiddenVisibility());
            }
        }
        return hiddenVisibilities;
    }

    private boolean isHidden(ByteSequence propertyKey, ByteSequence propertyName, ByteSequence propertyVisibility) {
        for (HiddenProperty hiddenProperty : hiddenProperties) {
            if (hiddenProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                return true;
            }
        }
        return false;
    }

    private boolean isPropertyDeleted(ByteSequence propertyKey, ByteSequence propertyName, long propertyTimestamp, ByteSequence propertyVisibility) {
        for (SoftDeletedProperty softDeletedProperty : softDeletedProperties) {
            if (softDeletedProperty.matches(propertyKey, propertyName, propertyVisibility)) {
                return softDeletedProperty.getTimestamp() >= propertyTimestamp;
            }
        }
        return false;
    }
}
