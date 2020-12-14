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
package com.mware.ge.accumulo.iterator;

import com.mware.ge.accumulo.iterator.util.SetOfStringsEncoder;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import com.mware.ge.ElementFilter;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class HasAuthorizationFilter extends Filter {
    private static final String SETTING_AUTHORIZATION_TO_MATCH = "authorizationToMatch";
    private static final String SETTING_FILTERS = "filters";
    private static final Pattern SPLIT_PATTERN = Pattern.compile("[^A-Za-z0-9_\\-\\.]");
    private String authorizationToMatch;
    private EnumSet<ElementFilter> filters;
    private Map<Text, Boolean> matchCache;

    public static void setAuthorizationToMatch(IteratorSetting settings, String authorizationToMatch) {
        settings.addOption(SETTING_AUTHORIZATION_TO_MATCH, authorizationToMatch);
    }

    public static void setFilters(IteratorSetting settings, EnumSet<ElementFilter> filters) {
        Set<String> filterStrings = new HashSet<>();
        for (ElementFilter filter : filters) {
            filterStrings.add(filter.name());
        }
        settings.addOption(SETTING_FILTERS, SetOfStringsEncoder.encodeToString(filterStrings));
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        authorizationToMatch = options.get(SETTING_AUTHORIZATION_TO_MATCH);
        Set<String> filterStrings = SetOfStringsEncoder.decodeFromString(options.get(SETTING_FILTERS));
        List<ElementFilter> filtersCollection = new ArrayList<>();
        for (String filterString : filterStrings) {
            filtersCollection.add(ElementFilter.valueOf(filterString));
        }
        filters = EnumSet.copyOf(filtersCollection);
        matchCache = new HashMap<>();
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        HasAuthorizationFilter filter = (HasAuthorizationFilter) super.deepCopy(env);
        filter.authorizationToMatch = this.authorizationToMatch;
        filter.filters = this.filters;
        filter.matchCache = new HashMap<>();
        return filter;
    }

    @Override
    public boolean accept(Key k, Value v) {
        if (filters.contains(ElementFilter.ELEMENT)
                && (k.getColumnFamily().equals(EdgeIterator.CF_SIGNAL) || k.getColumnFamily().equals(VertexIterator.CF_SIGNAL))
                && isMatch(k)) {
            return true;
        }

        if (filters.contains(ElementFilter.PROPERTY) && k.getColumnFamily().equals(EdgeIterator.CF_PROPERTY) && isMatch(k)) {
            return true;
        }

        if (filters.contains(ElementFilter.PROPERTY_METADATA) && k.getColumnFamily().equals(EdgeIterator.CF_PROPERTY_METADATA) && isMatch(k)) {
            return true;
        }

        return false;
    }

    private boolean isMatch(Key k) {
        Text columnVisibilityText = k.getColumnVisibility();
        if (columnVisibilityText.getLength() == 0) {
            return false;
        }
        Boolean match = matchCache.get(columnVisibilityText);
        if (match != null) {
            return match;
        }

        String[] parts = SPLIT_PATTERN.split(k.getColumnVisibilityParsed().toString());
        for (String part : parts) {
            if (part.equals(authorizationToMatch)) {
                matchCache.put(columnVisibilityText, true);
                return true;
            }
        }
        matchCache.put(columnVisibilityText, false);
        return false;
    }
}
