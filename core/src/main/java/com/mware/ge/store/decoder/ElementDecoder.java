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

import com.mware.ge.Authorizations;
import com.mware.ge.FetchHints;
import com.mware.ge.Visibility;
import com.mware.ge.collection.Pair;
import com.mware.ge.collection.PrefetchingIterator;
import com.mware.ge.security.ArrayByteSequence;
import com.mware.ge.security.ByteSequence;
import com.mware.ge.store.StorableElement;
import com.mware.ge.store.StorableGraph;
import com.mware.ge.store.StoreKey;
import com.mware.ge.store.StoreValue;
import com.mware.ge.store.mutations.ElementMutationBuilder;
import com.mware.ge.store.util.HiddenProperty;
import com.mware.ge.store.util.SoftDeletedProperty;
import com.mware.ge.util.LookAheadIterable;

import java.util.*;

public abstract class ElementDecoder<T extends ElementData> implements Iterable<T> {
    protected FetchHints fetchHints;
    protected T elementData;
    protected Authorizations authorizations;
    protected StorableGraph graph;
    private PrefetchingIterator<Pair<StoreKey, StoreValue>> storeIterable;

    public ElementDecoder(PrefetchingIterator<Pair<StoreKey, StoreValue>> storeIterable, StorableGraph graph, FetchHints fetchHints, Authorizations authorizations) {
        this.storeIterable = storeIterable;
        this.graph = graph;
        this.fetchHints = fetchHints;
        this.elementData = createElementData(graph);
        this.authorizations = authorizations;
    }

    @Override
    public Iterator<T> iterator() {
        return new LookAheadIterable<Pair<StoreKey, StoreValue>, T>() {
            @Override
            protected boolean isIncluded(Pair<StoreKey, StoreValue> src, T elem) {
                return elem != null;
            }

            @Override
            protected T convert(Pair<StoreKey, StoreValue> source) {
                List<Pair<StoreKey, StoreValue>> mutations = new ArrayList<>();
                mutations.add(source);

                while (storeIterable.hasNext()) {
                    Pair<StoreKey, StoreValue> next = storeIterable.peek();
                    if (next != null) {
                        String id = source.first().id();
                        String nextId = next.first().id();
                        if (id.equals(nextId)) {
                            mutations.add(storeIterable.next());
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                mutations.sort(
                        Comparator.<Pair<StoreKey, StoreValue>>comparingLong(m -> m.other().ts())
                                .reversed()
                );
                return decode(mutations);
            }

            @Override
            protected Iterator<Pair<StoreKey, StoreValue>> createIterator() {
                return storeIterable;
            }
        }.iterator();
    }

    protected boolean populateElementData(List<Pair<StoreKey, StoreValue>> mutations) {
        for (int i = 0; i < mutations.size(); i++) {
            if (!processKeyValue(mutations.get(i))) {
                return false;
            }
        }

        if (this.elementData.visibility == null) {
            return false;
        }

        if (this.elementData.softDeleteTimestamp >= this.elementData.timestamp) {
            return false;
        }

        if (fetchHints.isIncludeHidden())
            return true;

        for (String hiddenVis : elementData.hiddenVisibilities) {
            if (authorizations.canRead(new Visibility(hiddenVis)))
                return false;
        }

        return true;
    }

    private boolean processKeyValue(Pair<StoreKey, StoreValue> pair) {
        if (this.elementData.id == null) {
            this.elementData.id = pair.first().id();
        }

        StoreKey key = pair.first();
        StoreValue value = pair.other();

        if (key.cf().equals(StorableElement.CF_PROPERTY_METADATA)) {
            if (authorizations.canRead(key.visibility()))
                extractPropertyMetadata(pair);
            return true;
        }

        if (key.cf().equals(StorableElement.CF_PROPERTY)) {
            if (authorizations.canRead(key.visibility()))
                extractPropertyData(pair);
            return true;
        }

        if (key.cf().equals(StorableElement.CF_EXTENDED_DATA)) {
            this.elementData.extendedTableNames.add(new String(value.value()));
            return true;
        }

        if (key.cf().equals(getVisibilitySignal()) && value.ts() > elementData.timestamp) {
            if (authorizations.canRead(key.visibility())) {
                elementData.visibility = key.visibilityString();
                elementData.timestamp = value.ts();
                processSignalColumn(pair);
            }
            return true;
        }

        if (processColumn(pair)) {
            return true;
        }

        if (key.cf().equals(StorableElement.DELETE_ROW_COLUMN_FAMILY)
                && key.cq().equals(StorableElement.DELETE_ROW_COLUMN_QUALIFIER)
                && Arrays.equals(ElementMutationBuilder.DELETE_ROW_VALUE, value.value())) {
            return false;
        }

        if (key.cf().equals(StorableElement.CF_SOFT_DELETE)
                && key.cq().equals(StorableElement.CQ_SOFT_DELETE)
                && Arrays.equals(StorableElement.SOFT_DELETE_VALUE, value.value())) {
            elementData.softDeleteTimestamp = pair.other().ts();
            return true;
        }

        if (key.cf().equals(StorableElement.CF_PROPERTY_SOFT_DELETE)) {
            extractPropertySoftDelete(pair);
            return true;
        }

        if (key.cf().equals(StorableElement.CF_HIDDEN)) {
            this.elementData.hiddenVisibilities.add(key.visibilityString());
            return true;
        }

        if (key.cf().equals(StorableElement.CF_PROPERTY_HIDDEN)) {
            extractPropertyHidden(pair);
            return true;
        }

        return true;
    }

    protected abstract T createElementData(StorableGraph graph);

    protected abstract String getVisibilitySignal();

    protected abstract boolean processColumn(Pair<StoreKey, StoreValue> keyValue);

    protected void processSignalColumn(Pair<StoreKey, StoreValue> keyValue) {
    }

    private void extractPropertyData(Pair<StoreKey, StoreValue> keyValue) {
        long timestamp = keyValue.other().ts();
        PropertyColumnQualifierByteSequence propertyColumnQualifier =
                new PropertyColumnQualifierByteSequence(new ArrayByteSequence(keyValue.first().cq()));
        ByteSequence mapKey = propertyColumnQualifier.getDiscriminator(new ArrayByteSequence(keyValue.first().vis()), timestamp);
        if (shouldIncludeProperty(propertyColumnQualifier.getPropertyName())) {
            this.elementData.propertyColumnQualifiers.put(mapKey, propertyColumnQualifier);
            this.elementData.propertyValues.put(mapKey, keyValue.other().value());
            this.elementData.propertyVisibilities.put(mapKey, new ArrayByteSequence(keyValue.first().vis()));
            this.elementData.propertyTimestamps.put(mapKey, timestamp);
        }
    }

    private void extractPropertyMetadata(Pair<StoreKey, StoreValue> keyValue) {
        PropertyMetadataColumnQualifierByteSequence propertyMetadataColumnQualifier =
                new PropertyMetadataColumnQualifierByteSequence(new ArrayByteSequence(keyValue.first().cq()));

        if (shouldIncludeMetadata(propertyMetadataColumnQualifier)) {
            ByteSequence discriminator = propertyMetadataColumnQualifier.getPropertyDiscriminator(keyValue.other().ts());
            List<Integer> propertyMetadata = elementData.propertyMetadata.computeIfAbsent(discriminator, k -> new ArrayList<>());
            DecoderMetadataEntry pme = new DecoderMetadataEntry(
                    propertyMetadataColumnQualifier.getMetadataKey(),
                    new ArrayByteSequence(keyValue.first().vis()),
                    keyValue.other().value()
            );
            int pos = elementData.metadataEntries.indexOf(pme);
            if (pos < 0) {
                pos = elementData.metadataEntries.size();
                elementData.metadataEntries.add(pme);
            }
            propertyMetadata.add(pos);
        }
    }

    private void extractPropertySoftDelete(Pair<StoreKey, StoreValue> keyValue) {
        PropertyColumnQualifierByteSequence propertyColumnQualifier =
                new PropertyColumnQualifierByteSequence(new ArrayByteSequence(keyValue.first().cq()));
        SoftDeletedProperty softDeletedProperty = new SoftDeletedProperty(
                propertyColumnQualifier.getPropertyKey(),
                propertyColumnQualifier.getPropertyName(),
                keyValue.other().ts(),
                new ArrayByteSequence(keyValue.first().vis())
        );
        this.elementData.softDeletedProperties.add(softDeletedProperty);
    }

    private void extractPropertyHidden(Pair<StoreKey, StoreValue> keyValue) {
        if (Arrays.equals(keyValue.other().value(), StorableElement.HIDDEN_VALUE_DELETED)) {
            return;
        }
        PropertyHiddenColumnQualifierByteSequence propertyHiddenColumnQualifier =
                new PropertyHiddenColumnQualifierByteSequence(new ArrayByteSequence(keyValue.first().cq()));
        HiddenProperty hiddenProperty = new HiddenProperty(
                propertyHiddenColumnQualifier.getPropertyKey(),
                propertyHiddenColumnQualifier.getPropertyName(),
                propertyHiddenColumnQualifier.getPropertyVisibilityString(),
                new ArrayByteSequence(keyValue.first().vis())
        );
        this.elementData.hiddenProperties.add(hiddenProperty);
    }

    private boolean shouldIncludeProperty(ByteSequence propertyName) {
        if (fetchHints.isIncludeAllProperties()) {
            return true;
        }
        return fetchHints.getPropertyNamesToInclude() != null
                && fetchHints.getPropertyNamesToInclude().contains(propertyName.toString());
    }

    private boolean shouldIncludeMetadata(PropertyMetadataColumnQualifierByteSequence propertyMetadataColumnQualifier) {
        if (!shouldIncludeProperty(propertyMetadataColumnQualifier.getPropertyName())) {
            return false;
        }
        if (fetchHints.isIncludeAllPropertyMetadata()) {
            return true;
        }
        ByteSequence metadataKey = propertyMetadataColumnQualifier.getMetadataKey();
        return fetchHints.getMetadataKeysToInclude() != null
                && fetchHints.getMetadataKeysToInclude().contains(metadataKey.toString());
    }

    public T getElementData() {
        return elementData;
    }

    public FetchHints getFetchHints() {
        return fetchHints;
    }

    protected T decode(List<Pair<StoreKey, StoreValue>> mutations) {
        elementData = createElementData(graph);

        if (populateElementData(mutations)) {
            return this.getElementData();
        } else {
            return null;
        }
    }
}
