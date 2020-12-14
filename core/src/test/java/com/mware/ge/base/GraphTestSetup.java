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
package com.mware.ge.base;

import com.mware.ge.*;
import com.mware.ge.query.DefaultGraphQuery;
import com.mware.ge.query.Query;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.aggregations.TermsAggregation;
import com.mware.ge.query.aggregations.TermsBucket;
import com.mware.ge.query.aggregations.TermsResult;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.DateValue;
import org.junit.Rule;
import org.junit.rules.MethodRule;
import org.junit.runners.model.Statement;

import java.time.ZoneOffset;
import java.util.*;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static org.junit.Assume.assumeTrue;

public interface GraphTestSetup {
    String VISIBILITY_A_STRING = "a";
    String VISIBILITY_B_STRING = "b";
    String VISIBILITY_C_STRING = "c";
    String VISIBILITY_MIXED_CASE_STRING = "MIXED_CASE_a";
    Visibility VISIBILITY_A = new Visibility(VISIBILITY_A_STRING);
    Visibility VISIBILITY_A_AND_B = new Visibility("a&b");
    Visibility VISIBILITY_B = new Visibility(VISIBILITY_B_STRING);
    Visibility VISIBILITY_C = new Visibility(VISIBILITY_C_STRING);
    Visibility VISIBILITY_MIXED_CASE_a = new Visibility("((MIXED_CASE_a))|b");
    Visibility VISIBILITY_EMPTY = new Visibility("");
    String LABEL_LABEL1 = "label1";
    String LABEL_LABEL2 = "label2";
    String LABEL_LABEL3 = "label3";
    String LABEL_BAD = "bad";
    Authorizations AUTHORIZATIONS_A = new Authorizations("a");
    Authorizations AUTHORIZATIONS_B = new Authorizations("b");
    Authorizations AUTHORIZATIONS_C = new Authorizations("c");
    Authorizations AUTHORIZATIONS_MIXED_CASE_a_AND_B = new Authorizations("MIXED_CASE_a", "b");
    Authorizations AUTHORIZATIONS_A_AND_B = new Authorizations("a", "b");
    Authorizations AUTHORIZATIONS_B_AND_C = new Authorizations("b", "c");
    Authorizations AUTHORIZATIONS_A_AND_B_AND_C = new Authorizations("a", "b", "c");
    Authorizations AUTHORIZATIONS_EMPTY = new Authorizations();
    Authorizations AUTHORIZATIONS_BAD = new Authorizations("bad");
    Authorizations AUTHORIZATIONS_ALL = new Authorizations("a", "b", "c", "MIXED_CASE_a");
    int LARGE_PROPERTY_VALUE_SIZE = 1024 * 1024 + 1;

    <T extends Graph> T getGraph();
    TestGraphFactory graphFactory();

    default void addAuthorizations(String... authorizations) {
        getGraph().createAuthorizations(authorizations);
    }

    @Rule
    MethodRule skipRule = (base, method, target) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            try {
                base.evaluate();
            } catch (GeNotSupportedException e) {
                System.out.println("Ignoring method: " + method.getName());
                assumeTrue(false);
            }
        }
    };

    /**
     * This is to mitigate a difference in date math between the Joda/Groovy and Painless scripting languages.
     * The only known difference is when calculating the WEEK_OF_YEAR. Painless appears to begin the week
     * on Monday while Joda/Groovy appear to use Sunday.
     *
     * @return true if date math is performed using the painless scripting language
     */
    default boolean isPainlessDateMath() {
        return false;
    }


    default List<Vertex> sortById(List<Vertex> vertices) {
        Collections.sort(vertices, Comparator.comparing(Element::getId));
        return vertices;
    }

    default boolean isInputStreamMarkResetSupported() {
        return true;
    }

    default DateValue createDate(int year, int month, int day) {
        return DateValue.date(year, month, day);
    }

    default DateTimeValue createDate(int year, int month, int day, int hour, int min, int sec) {
        return DateTimeValue.datetime(year, month, day, hour, min, sec, 0, ZoneOffset.UTC);
    }

    default Map<Object, Long> termsBucketToMap(Iterable<TermsBucket> buckets) {
        Map<Object, Long> results = new HashMap<>();
        for (TermsBucket b : buckets) {
            results.put(b.getKey(), b.getCount());
        }
        return results;
    }

    default List<Vertex> getVertices(long count) {
        List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Vertex vertex = getGraph().addVertex(Integer.toString(i), VISIBILITY_EMPTY, AUTHORIZATIONS_EMPTY, CONCEPT_TYPE_THING);
            vertices.add(vertex);
        }
        return vertices;
    }

    default Map<Object, Long> queryGraphQueryWithTermsAggregation(String propertyName, ElementType elementType, Authorizations authorizations) {
        return queryGraphQueryWithTermsAggregation(null, propertyName, elementType, authorizations);
    }

    default Map<Object, Long> queryGraphQueryWithTermsAggregation(String queryString, String propertyName, ElementType elementType, Authorizations authorizations) {
        TermsResult aggregationResult = queryGraphQueryWithTermsAggregationResult(queryString, propertyName, elementType, null, authorizations);
        return termsBucketToMap(aggregationResult.getBuckets());
    }

    default TermsResult queryGraphQueryWithTermsAggregationResult(String propertyName, ElementType elementType, Authorizations authorizations) {
        return queryGraphQueryWithTermsAggregationResult(null, propertyName, elementType, null, authorizations);
    }

    default TermsResult queryGraphQueryWithTermsAggregationResult(String queryString, String propertyName, ElementType elementType, Integer buckets, Authorizations authorizations) {
        Query q = (queryString == null ? getGraph().query(authorizations) : getGraph().query(queryString, authorizations)).limit(0);
        TermsAggregation agg = new TermsAggregation("terms-count", propertyName);
        if (buckets != null) {
            agg.setSize(buckets);
        }
        if (!q.isAggregationSupported(agg)) {
            System.err.println(String.format("%s unsupported", agg.getClass().getName()));
            return null;
        }
        q.addAggregation(agg);
        QueryResultsIterable<? extends Element> elements = elementType == ElementType.VERTEX ? q.vertices() : q.edges();
        return elements.getAggregationResult("terms-count", TermsResult.class);
    }

    default boolean disableEdgeIndexing(Graph graph) {
        return false;
    }

    default boolean isLuceneQueriesSupported() {
        return !(getGraph().query(AUTHORIZATIONS_A) instanceof DefaultGraphQuery);
    }
}
