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
package com.mware.ge.id;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleNameSubstitutionStrategy implements NameSubstitutionStrategy {
    private List<DeflateItem> deflateSubstitutionList = new ArrayList<>();
    private final Cache<String, String> deflateCache;
    private long deflateCalls;
    private long deflateCacheMisses;
    private List<InflateItem> inflateSubstitutionList = new ArrayList<>();
    private final Cache<String, String> inflateCache;
    private long inflateCalls;
    private long inflateCacheMisses;
    public static final String SUBS_DELIM = "\u0002";

    public SimpleNameSubstitutionStrategy() {
        deflateCache = Cache2kBuilder.of(String.class, String.class)
                .name(SimpleNameSubstitutionStrategy.class, "deflateCache-" + System.identityHashCode(this))
                .entryCapacity(10000)
                .eternal(true)
                .loader(value -> {
                    deflateCacheMisses++;
                    String deflatedVal = value;
                    for (DeflateItem deflateItem : deflateSubstitutionList) {
                        deflatedVal = deflateItem.deflate(deflatedVal);
                    }
                    return deflatedVal;
                })
                .build();

        inflateCache = Cache2kBuilder.of(String.class, String.class)
                .name(SimpleNameSubstitutionStrategy.class, "inflateCache-" + System.identityHashCode(this))
                .entryCapacity(10000)
                .eternal(true)
                .loader(value -> {
                    inflateCacheMisses++;
                    String inflatedValue = value;
                    for (InflateItem inflateItem : inflateSubstitutionList) {
                        inflatedValue = inflateItem.inflate(inflatedValue);
                    }
                    return inflatedValue;
                })
                .build();
    }

    @Override
    public void setup(Map config) {
        this.setSubstitutionList(SimpleSubstitutionUtils.getSubstitutionList(config));
    }

    @Override
    public String deflate(String value) {
        deflateCalls++;
        return deflateCache.get(value);
    }

    @Override
    public String inflate(String value) {
        inflateCalls++;
        return inflateCache.get(value);
    }

    public static String wrap(String str) {
        return SUBS_DELIM + str + SUBS_DELIM;
    }

    public void setSubstitutionList(List<Pair<String, String>> substitutionList) {
        this.inflateSubstitutionList.clear();
        this.deflateSubstitutionList.clear();
        for (Pair<String, String> pair : substitutionList) {
            this.inflateSubstitutionList.add(new InflateItem(wrap(pair.getValue()), pair.getKey()));
            this.deflateSubstitutionList.add(new DeflateItem(pair.getKey(), wrap(pair.getValue())));
        }
    }

    public long getDeflateCalls() {
        return deflateCalls;
    }

    public long getDeflateCacheMisses() {
        return deflateCacheMisses;
    }

    public long getInflateCalls() {
        return inflateCalls;
    }

    public long getInflateCacheMisses() {
        return inflateCacheMisses;
    }

    private static class InflateItem {
        private final String pattern;
        private final String replacement;

        public InflateItem(String pattern, String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }

        public String inflate(String value) {
            return StringUtils.replace(value, pattern, replacement);
        }
    }

    private static class DeflateItem {
        private final String pattern;
        private final String replacement;

        public DeflateItem(String pattern, String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }

        public String deflate(String value) {
            return StringUtils.replace(value, pattern, replacement);
        }
    }
}
