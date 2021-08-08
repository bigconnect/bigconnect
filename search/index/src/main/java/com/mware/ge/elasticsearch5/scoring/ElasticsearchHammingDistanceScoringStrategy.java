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
package com.mware.ge.elasticsearch5.scoring;

import com.mware.ge.Authorizations;
import com.mware.ge.PropertyDefinition;
import com.mware.ge.values.storable.TextValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import com.mware.ge.Graph;
import com.mware.ge.GeException;
import com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex;
import com.mware.ge.query.QueryParameters;
import com.mware.ge.scoring.HammingDistanceScoringStrategy;
import com.mware.ge.util.IOUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticsearchHammingDistanceScoringStrategy
        extends HammingDistanceScoringStrategy
        implements ElasticsearchScoringStrategy {
    private final String scriptSrc;

    public ElasticsearchHammingDistanceScoringStrategy(String field, String hash) {
        super(field, hash);
        try {
            scriptSrc = IOUtils.toString(getClass().getResourceAsStream("hamming-distance.painless"));
        } catch (Exception ex) {
            throw new GeException("Could not load painless script", ex);
        }
    }

    @Override
    public QueryBuilder updateElasticsearchQuery(
            Graph graph,
            Elasticsearch5SearchIndex searchIndex,
            QueryBuilder query,
            Authorizations authorizations
    ) {
        List<String> fieldNames = getFieldNames(graph, searchIndex, authorizations, getField());
        if (fieldNames == null) {
            return query;
        }

        HashMap<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("hash", getHash());
        scriptParams.put("fieldNames", fieldNames);
        Script script = new Script(ScriptType.INLINE, "painless", scriptSrc, scriptParams);
        return QueryBuilders.functionScoreQuery(query, new ScriptScoreFunctionBuilder(script));
    }

    private List<String> getFieldNames(
            Graph graph,
            Elasticsearch5SearchIndex searchIndex,
            Authorizations authorizations,
            String field
    ) {
        PropertyDefinition propertyDefinition = graph.getPropertyDefinition(field);
        if (propertyDefinition == null) {
            return null;
        }
        if (!searchIndex.isPropertyInIndex(graph, field)) {
            return null;
        }
        if (!searchIndex.supportsExactMatchSearch(propertyDefinition)) {
            return null;
        }

        String[] propertyNames = searchIndex.getPropertyNames(
                graph,
                propertyDefinition.getPropertyName(),
                authorizations
        );
        return Arrays.stream(propertyNames)
                .filter(propertyName -> TextValue.class.isAssignableFrom(propertyDefinition.getDataType()))
                .map(propertyName -> propertyName + Elasticsearch5SearchIndex.EXACT_MATCH_PROPERTY_NAME_SUFFIX)
                .collect(Collectors.toList());
    }
}
