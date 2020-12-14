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

import java.util.ArrayList;
import java.util.Collection;

public interface Metadata {
    static Metadata create(FetchHints fetchHints) {
        return new MapMetadata(fetchHints);
    }

    static Metadata create() {
        return new MapMetadata();
    }

    static Metadata create(Metadata metadata) {
        return new MapMetadata(metadata);
    }

    static Metadata create(Metadata metadata, FetchHints fetchHints) {
        return new MapMetadata(metadata, fetchHints);
    }

    Metadata add(String key, Value value, Visibility visibility);

    void remove(String key, Visibility visibility);

    void clear();

    void remove(String key);

    Collection<Entry> entrySet();

    Entry getEntry(String key, Visibility visibility);

    Entry getEntry(String key);

    Collection<Entry> getEntries(String key);

    FetchHints getFetchHints();

    default Object getValue(String key, Visibility visibility) {
        Entry entry = getEntry(key, visibility);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    default Value getValue(String key) {
        Entry entry = getEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getValue();
    }

    default Collection<Value> getValues(String key) {
        Collection<Value> results = new ArrayList<>();
        Collection<Metadata.Entry> entries = getEntries(key);
        for (Metadata.Entry entry : entries) {
            results.add(entry.getValue());
        }
        return results;
    }

    default boolean containsKey(String key) {
        return getEntries(key).size() > 0;
    }

    default boolean contains(String key, Visibility visibility) {
        return getEntry(key, visibility) != null;
    }

    interface Entry {
        String getKey();

        Value getValue();

        Visibility getVisibility();
    }
}
