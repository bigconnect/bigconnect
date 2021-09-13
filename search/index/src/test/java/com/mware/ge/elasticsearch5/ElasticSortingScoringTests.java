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
package com.mware.ge.elasticsearch5;

import com.mware.ge.Graph;
import com.mware.ge.GraphWithSearchIndex;
import com.mware.ge.Vertex;
import com.mware.ge.base.GraphSortingScoringTests;
import com.mware.ge.base.GraphTestSetup;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.elasticsearch5.scoring.ElasticsearchFieldValueScoringStrategy;
import com.mware.ge.elasticsearch5.scoring.ElasticsearchHammingDistanceScoringStrategy;
import com.mware.ge.elasticsearch5.sorting.ElasticsearchLengthOfStringSortingStrategy;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.SortDirection;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.scoring.ScoringStrategy;
import com.mware.ge.sorting.SortingStrategy;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import static com.mware.ge.util.IterableUtils.count;

public class ElasticSortingScoringTests extends GraphSortingScoringTests implements GraphTestSetup  {
    @ClassRule
    public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(ElasticGeoTests.class.getName());

    @Override
    public TestGraphFactory graphFactory() {
        return new ElasticGraphFactory().
                withElasticsearchResource(elasticsearchResource);
    }

    @Override
    public void before() throws Exception {
        elasticsearchResource.dropIndices();
        super.before();
    }

    @Test
    @Override
    public void testGraphQuerySortOnPropertyThatHasNoValuesInTheIndex() {
        super.testGraphQuerySortOnPropertyThatHasNoValuesInTheIndex();

        getSearchIndex().clearCache();

        QueryResultsIterable<Vertex> vertices = graph.query(
                GeQueryBuilders.searchAll()
                        .sort("age", SortDirection.ASCENDING),
                AUTHORIZATIONS_A
        ).vertices();
        Assert.assertEquals(2, count(vertices));
    }

    private Elasticsearch5SearchIndex getSearchIndex() {
        return (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
    }

    @Override
    public boolean disableEdgeIndexing(Graph graph) {
        Elasticsearch5SearchIndex searchIndex = (Elasticsearch5SearchIndex) ((GraphWithSearchIndex) graph).getSearchIndex();
        searchIndex.getConfig().getGraphConfiguration().set(ElasticsearchOptions.INDEX_EDGES.name(), false);
        return true;
    }

    @Override
    public boolean isLuceneQueriesSupported() {
        return true;
    }

    @Override
    public boolean isPainlessDateMath() {
        return true;
    }


    @Override
    protected ScoringStrategy getHammingDistanceScoringStrategy(String field, String hash) {
        return new ElasticsearchHammingDistanceScoringStrategy(field, hash);
    }

    @Override
    protected SortingStrategy getLengthOfStringSortingStrategy(String propertyName) {
        return new ElasticsearchLengthOfStringSortingStrategy(propertyName);
    }

    @Override
    protected ScoringStrategy getFieldValueScoringStrategy(String field) {
        return new ElasticsearchFieldValueScoringStrategy(field);
    }
}
