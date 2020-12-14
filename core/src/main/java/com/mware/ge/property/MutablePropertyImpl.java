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
package com.mware.ge.property;

import com.mware.ge.*;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.mware.ge.util.IncreasingTime;
import com.mware.ge.values.storable.Value;

import java.util.*;

public class MutablePropertyImpl extends MutableProperty {
    private final String key;
    private final String name;
    private final FetchHints fetchHints;
    private Set<Visibility> hiddenVisibilities;
    private Value value;
    private Visibility visibility;
    private Long timestamp;
    private final Metadata metadata;

    public MutablePropertyImpl(
            String key,
            String name,
            Value value,
            Metadata metadata,
            Long timestamp,
            Set<Visibility> hiddenVisibilities,
            Visibility visibility,
            FetchHints fetchHints
    ) {
        if (metadata == null && fetchHints.isIncludePropertyMetadata()) {
            metadata = Metadata.create(FetchHints.ALL);
        }

        this.key = key;
        this.name = name;
        this.value = value;
        this.metadata = metadata;
        this.timestamp = timestamp;
        this.visibility = visibility;
        this.hiddenVisibilities = hiddenVisibilities;
        this.fetchHints = fetchHints;
    }

    public String getKey() {
        return key;
    }

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

    public Visibility getVisibility() {
        return visibility;
    }

    @Override
    public FetchHints getFetchHints() {
        return fetchHints;
    }

    public Metadata getMetadata() {
        if (!fetchHints.isIncludePropertyMetadata()) {
            throw new GeMissingFetchHintException(fetchHints, "includePropertyMetadata");
        }
        return metadata;
    }

    @Override
    public Iterable<Visibility> getHiddenVisibilities() {
        if (this.hiddenVisibilities == null) {
            return new ArrayList<>();
        }
        return this.hiddenVisibilities;
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

    public void setValue(Value value) {
        this.value = value;
    }

    @Override
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setVisibility(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public void addHiddenVisibility(Visibility visibility) {
        if (this.hiddenVisibilities == null) {
            this.hiddenVisibilities = new HashSet<>();
        }
        this.hiddenVisibilities.add(visibility);
    }

    @Override
    public void removeHiddenVisibility(Visibility visibility) {
        if (this.hiddenVisibilities == null) {
            this.hiddenVisibilities = new HashSet<>();
        }
        this.hiddenVisibilities.remove(visibility);
    }

    @Override
    protected void updateMetadata(Property property) {
        Collection<Metadata.Entry> entries = new ArrayList<>(property.getMetadata().entrySet());
        this.metadata.clear();
        for (Metadata.Entry metadataEntry : entries) {
            this.metadata.add(metadataEntry.getKey(), metadataEntry.getValue(), metadataEntry.getVisibility());
        }
    }
}
