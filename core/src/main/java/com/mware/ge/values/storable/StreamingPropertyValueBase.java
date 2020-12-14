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
package com.mware.ge.values.storable;

import com.mware.ge.hashing.HashFunction;

import java.io.Serializable;

public abstract class StreamingPropertyValueBase extends Value implements Serializable {
    protected boolean store = true; // Need to keep for Kryo serialization
    protected boolean searchIndex = true;

    public StreamingPropertyValueBase() {
    }

    @SuppressWarnings("unchecked")
    public <T extends StreamingPropertyValueBase> T searchIndex(boolean searchIndex) {
        this.searchIndex = searchIndex;
        return (T) this;
    }

    public boolean isSearchIndex() {
        return searchIndex;
    }


    @Override
    public boolean equals(Value other) {
        return false;
    }

    @Override
    public ValueGroup valueGroup() {
        return ValueGroup.STREAMING;
    }

    @Override
    public NumberType numberType() {
        return NumberType.NO_NUMBER;
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        return 0;
    }

    @Override
    protected int computeHash() {
        return 0;
    }
}
