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
package com.mware.core.model.properties.types;

import com.mware.ge.Metadata;
import com.mware.ge.Property;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.Value;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class MetadataBcProperty<TRaw> {
    private final String metadataKey;

    protected MetadataBcProperty(String metadataKey) {
        this.metadataKey = metadataKey;
    }

    /**
     * Convert the raw value to an appropriate value for storage
     * in Ge.
     */
    public abstract Value wrap(final TRaw value);

    /**
     * Convert the Ge value to its original raw type.
     */
    public abstract TRaw unwrap(final Value value);

    public String getMetadataKey() {
        return metadataKey;
    }

    public TRaw getMetadataValue(Metadata metadata) {
        return unwrap(metadata.getValue(getMetadataKey()));
    }

    public Collection<TRaw> getMetadataValues(Metadata metadata) {
        return metadata.getValues(getMetadataKey()).stream().map(this::unwrap).collect(Collectors.toList());
    }

    public TRaw getMetadataValueOrDefault(Metadata metadata, TRaw defaultValue) {
        Value value = metadata.getValue(getMetadataKey());
        if (value == null) {
            return defaultValue;
        }
        return unwrap(value);
    }

    public TRaw getMetadataValue(Metadata metadata, TRaw defaultValue) {
        if (metadata.getEntry(getMetadataKey()) == null) {
            return defaultValue;
        }
        return unwrap(metadata.getValue(getMetadataKey()));
    }

    public TRaw getMetadataValue(Map<String, Object> metadata) {
        //noinspection unchecked
        return (TRaw) metadata.get(getMetadataKey());
    }

    public TRaw getMetadataValue(Property property) {
        return getMetadataValue(property.getMetadata());
    }

    public void setMetadata(Metadata metadata, TRaw value, Visibility visibility) {
        metadata.add(getMetadataKey(), wrap(value), visibility);
    }

    public void setMetadata(PropertyMetadata metadata, TRaw value, Visibility visibility) {
        metadata.add(getMetadataKey(), wrap(value), visibility);
    }

    public void setMetadata(ExistingElementMutation m, Property property, TRaw value, Visibility visibility) {
        m.setPropertyMetadata(property, getMetadataKey(), wrap(value), visibility);
    }
}
