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
package com.mware.ge.accumulo;

import com.mware.ge.accumulo.iterator.util.ByteSequenceUtils;
import com.mware.ge.id.IdentityNameSubstitutionStrategy;
import com.mware.ge.id.NameSubstitutionStrategy;
import org.apache.accumulo.core.data.ByteSequence;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import java.util.Map;

public class AccumuloNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private final NameSubstitutionStrategy nameSubstitutionStrategy;
    private final Cache<ByteSequence, String> inflateTextCache;

    protected AccumuloNameSubstitutionStrategy(NameSubstitutionStrategy nameSubstitutionStrategy) {
        this.nameSubstitutionStrategy = nameSubstitutionStrategy;
        inflateTextCache = Cache2kBuilder.of(ByteSequence.class, String.class)
                .name(AccumuloNameSubstitutionStrategy.class, "inflateTextCache-" + System.identityHashCode(this))
                .entryCapacity(10000)
                .eternal(true)
                .loader(byteSequence -> inflate(ByteSequenceUtils.toString(byteSequence)))
                .build();
    }

    @Override
    public void setup(Map config) {
        return;
    }

    @Override
    public String deflate(String value) {
        return this.nameSubstitutionStrategy.deflate(value);
    }

    @Override
    public String inflate(String value) {
        return this.nameSubstitutionStrategy.inflate(value);
    }

    public String inflate(ByteSequence text) {
        if (text == null) {
            return null;
        }
        if (this.nameSubstitutionStrategy instanceof IdentityNameSubstitutionStrategy) {
            return text.toString();
        }
        return inflateTextCache.get(text);
    }

    public static AccumuloNameSubstitutionStrategy create(NameSubstitutionStrategy nameSubstitutionStrategy) {
        if (nameSubstitutionStrategy instanceof AccumuloNameSubstitutionStrategy) {
            return (AccumuloNameSubstitutionStrategy) nameSubstitutionStrategy;
        }
        return new AccumuloNameSubstitutionStrategy(nameSubstitutionStrategy);
    }
}
