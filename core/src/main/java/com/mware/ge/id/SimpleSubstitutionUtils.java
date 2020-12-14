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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleSubstitutionUtils {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(SimpleSubstitutionUtils.class);
    public static final String SUBSTITUTION_MAP_PREFIX = "substitution";
    public static final String KEY_IDENTIFIER = "key";
    public static final String VALUE_IDENTIFIER = "value";

    public static List<Pair<String, String>> getSubstitutionList(Map configuration) {
        Map<String, MutablePair<String, String>> substitutionMap = Maps.newHashMap();

        //parse the config arguments
        for (Object objKey : configuration.keySet()) {
            String key = objKey.toString();
            if (key.startsWith(SUBSTITUTION_MAP_PREFIX + ".")) {
                List<String> parts = Lists.newArrayList(IterableUtils.toList(Splitter.on('.').split(key)));
                String pairKey = parts.get(parts.size() - 2);
                String valueType = parts.get(parts.size() - 1);

                if (!substitutionMap.containsKey(pairKey)) {
                    substitutionMap.put(pairKey, new MutablePair<String, String>());
                }

                MutablePair<String, String> pair = substitutionMap.get(pairKey);

                if (KEY_IDENTIFIER.equals(valueType)) {
                    pair.setLeft(configuration.get(key).toString());
                } else if (VALUE_IDENTIFIER.equals(valueType)) {
                    pair.setValue(configuration.get(key).toString());
                }
            }
        }

        //order is important, so create order by the pairKey that was in the config.  eg: substitution.0.key is before substitution.1.key so it is evaluated in that order
        List<String> keys = Lists.newArrayList(substitutionMap.keySet());
        Collections.sort(keys);

        List<Pair<String, String>> finalMap = Lists.newArrayList();
        for (String key : keys) {
            Pair<String, String> pair = substitutionMap.get(key);
            finalMap.add(pair);
            LOGGER.info("Using substitution %s -> %s", pair.getKey(), pair.getValue());
        }

        return finalMap;
    }
}
