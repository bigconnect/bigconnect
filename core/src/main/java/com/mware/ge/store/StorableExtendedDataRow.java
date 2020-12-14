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
package com.mware.ge.store;

import com.mware.ge.*;

import java.util.ArrayList;
import java.util.Set;

public class StorableExtendedDataRow extends ExtendedDataRowBase {
    private final ExtendedDataRowId rowId;
    private final Set<Property> properties;

    public StorableExtendedDataRow(
            ExtendedDataRowId rowId,
            Set<Property> properties,
            FetchHints fetchHints
    ) {
        super(fetchHints);
        this.rowId = rowId;
        this.properties = properties;
    }

    @Override
    public ExtendedDataRowId getId() {
        return rowId;
    }

    @Override
    public Iterable<Property> getProperties() {
        return properties;
    }

    public static class StorableExtendedDataRowProperty extends Property {
        private final String propertyName;
        private final String propertyKey;
        private final com.mware.ge.values.storable.Value propertyValue;
        private final FetchHints fetchHints;
        private final long timestamp;
        private final Visibility visibility;

        public StorableExtendedDataRowProperty(
                String propertyName,
                String propertyKey,
                com.mware.ge.values.storable.Value propertyValue,
                FetchHints fetchHints,
                long timestamp,
                Visibility visibility
        ) {
            this.propertyName = propertyName;
            this.propertyKey = propertyKey;
            this.propertyValue = propertyValue;
            this.fetchHints = fetchHints;
            this.timestamp = timestamp;
            this.visibility = visibility;
        }

        @Override
        public String getKey() {
            return propertyKey;
        }

        @Override
        public String getName() {
            return propertyName;
        }

        @Override
        public com.mware.ge.values.storable.Value getValue() {
            return propertyValue;
        }

        @Override
        public Long getTimestamp() {
            return timestamp;
        }

        @Override
        public Visibility getVisibility() {
            return visibility;
        }

        @Override
        public Metadata getMetadata() {
            return Metadata.create(fetchHints);
        }

        @Override
        public FetchHints getFetchHints() {
            return fetchHints;
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
