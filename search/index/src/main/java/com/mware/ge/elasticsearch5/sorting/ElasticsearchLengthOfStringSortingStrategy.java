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
package com.mware.ge.elasticsearch5.sorting;

import com.mware.ge.Authorizations;
import com.mware.ge.GeException;
import com.mware.ge.Graph;
import com.mware.ge.PropertyDefinition;
import com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex;
import com.mware.ge.query.SortDirection;
import com.mware.ge.sorting.LengthOfStringSortingStrategy;
import com.mware.ge.util.IOUtils;
import com.mware.ge.values.storable.TextValue;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchLengthOfStringSortingStrategy
        extends LengthOfStringSortingStrategy
        implements ElasticsearchSortingStrategy {
    public static final String SCRIPT_NAME = "length-of-string.painless";
    private final String scriptSource;

    public ElasticsearchLengthOfStringSortingStrategy(String propertyName) {
        super(propertyName);
        try {
            scriptSource = IOUtils.toString(getClass().getResourceAsStream(SCRIPT_NAME));
        } catch (Exception ex) {
            throw new GeException("Could not load painless script: " + SCRIPT_NAME, ex);
        }
    }

    @Override
    public void updateElasticsearchQuery(
            Graph graph,
            Elasticsearch5SearchIndex searchIndex,
            SearchRequestBuilder q,
            Authorizations authorizations,
            SortDirection direction
    ) {
        PropertyDefinition propertyDefinition = graph.getPropertyDefinition(getPropertyName());

        SortOrder esOrder = direction == SortDirection.ASCENDING ? SortOrder.ASC : SortOrder.DESC;
        Map<String, Object> scriptParams = new HashMap<>();
        String[] propertyNames = searchIndex.getPropertyNames(graph, getPropertyName(), authorizations);
        List<String> fieldNames = Arrays.stream(propertyNames)
                .map(propertyName -> {
                    String suffix = propertyDefinition.getDataType().isAssignableFrom(TextValue.class)
                            ? Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX
                            : "";
                    return propertyName + suffix;
                })
                .collect(Collectors.toList());
        scriptParams.put("fieldNames", fieldNames);
        scriptParams.put("direction", esOrder.name());
        Script script = new Script(ScriptType.INLINE, "painless", scriptSource, scriptParams);
        ScriptSortBuilder.ScriptSortType sortType = ScriptSortBuilder.ScriptSortType.NUMBER;
        q.addSort(SortBuilders.scriptSort(script, sortType).order(SortOrder.ASC));
    }
}
