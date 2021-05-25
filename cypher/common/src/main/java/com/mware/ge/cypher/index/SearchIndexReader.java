/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.ge.cypher.index;

import com.drew.lang.Iterables;
import com.mware.ge.GeException;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.cypher.schema.IndexDescriptor;
import com.mware.ge.cypher.util.NodeValueIndexCursor;
import com.mware.ge.query.*;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.virtual.NodeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static com.mware.ge.cypher.index.IndexQuery.IndexQueryType.exact;
import static java.lang.String.format;

public class SearchIndexReader {
    private GeCypherQueryContext queryContext;

    public SearchIndexReader(GeCypherQueryContext queryContext) {
        this.queryContext = queryContext;
    }

    public NodeValueIndexCursor scan(IndexReference index, IndexOrder indexOrder, boolean needsValues) {
        String firstProperty = index.properties()[0];
        String conceptType = index.schema().keyId();
        Query query = queryContext.getGraph().query(queryContext.getAuthorizations());
        query.hasConceptType(conceptType);
        query.has(firstProperty);

        addSortToQuery(query, index, indexOrder);

        return new GeNodeValueIndexCursor(query.vertexIds());
    }

    public NodeValueIndexCursor seek(IndexReference index, IndexOrder indexOrder, boolean needsValues, IndexQuery... predicates) {
        Query query = toGeQuery(index, indexOrder, predicates);
        return new GeNodeValueIndexCursor(query.vertexIds());
    }

    private Query toGeQuery(IndexReference index, IndexOrder indexOrder, IndexQuery... predicates) {
        IndexQuery predicate = predicates[0];
        String conceptType = index.schema().keyId();
        Query query = queryContext.getGraph().query(queryContext.getAuthorizations());
        query.hasConceptType(conceptType);

        addSortToQuery(query, index, indexOrder);

        switch (predicate.type()) {
            case exact:
                for (IndexQuery predicate1 : predicates) {
                    assert predicate1.type() == exact :
                            "Exact followed by another query predicate type is not supported at this moment.";
                    IndexQuery.ExactPredicate p = (IndexQuery.ExactPredicate) predicate1;
                    String propName = p.propertyKeyId();
                    Value v = ((IndexQuery.ExactPredicate) predicate1).value();
                    query.has(propName, Compare.EQUAL, v);
                }
                return query;
            case exists:
                for (IndexQuery p : predicates) {
                    if (p.type() != IndexQuery.IndexQueryType.exists) {
                        throw new GeException(
                                "Exists followed by another query predicate type is not supported.");
                    }
                    String propName = p.propertyKeyId();
                    query.has(propName);
                }
                return query;
            case range:
                assertNotComposite(predicates);
                switch (predicate.valueGroup()) {
                    case NUMBER:
                        IndexQuery.NumberRangePredicate np = (IndexQuery.NumberRangePredicate) predicate;
                        String propName = np.propertyKeyId();
                        return query.range(propName, np.from(), np.to());
                    case TEXT:
                        IndexQuery.TextRangePredicate sp = (IndexQuery.TextRangePredicate) predicate;
                        propName = sp.propertyKeyId();
                        return query.range(propName, sp.from(), sp.to());
                    case DATE:
                    case ZONED_DATE_TIME:
                        IndexQuery.RangePredicate<DateTimeValue> rp = (IndexQuery.RangePredicate<DateTimeValue>) predicate;
                        propName = rp.propertyKeyId();
                        return query.range(propName, rp.fromValue(), rp.toValue());
                    default:
                        throw new UnsupportedOperationException(
                                format("Range scans of value group %s are not supported", predicate.valueGroup()));
                }
            case stringPrefix:
                assertNotComposite(predicates);
                IndexQuery.StringPrefixPredicate spp = (IndexQuery.StringPrefixPredicate) predicate;
                String propName = spp.propertyKeyId();
                return query.has(propName, Compare.STARTS_WITH, spp.prefix());
            case stringContains:
                assertNotComposite(predicates);
                IndexQuery.StringContainsPredicate scp = (IndexQuery.StringContainsPredicate) predicate;
                propName = scp.propertyKeyId();
                return query.has(propName, TextPredicate.CONTAINS, scp.contains());
            case stringSuffix:
                assertNotComposite(predicates);
                IndexQuery.StringSuffixPredicate ssp = (IndexQuery.StringSuffixPredicate) predicate;
                propName = ssp.propertyKeyId();
                return query.has(propName, Compare.ENDS_WITH, ssp.suffix());
            default:
                throw new RuntimeException("Index query not supported: " + Arrays.toString(predicates));
        }
    }

    private void addSortToQuery(Query query, IndexReference index, IndexOrder indexOrder) {
        switch (indexOrder) {
            case ASCENDING:
                query.sort(index.schema().getPropertyId(), SortDirection.ASCENDING);
                break;
            case DESCENDING:
                query.sort(index.schema().getPropertyId(), SortDirection.DESCENDING);
                break;
            case NONE:
        }
    }

    private void assertNotComposite(IndexQuery[] predicates) {
        assert predicates.length == 1 : "composite indexes not yet supported for this operation";
    }

    class GeNodeValueIndexCursor implements NodeValueIndexCursor, NodeValueClient {
        private boolean needsValues;
        private boolean closed;
        private QueryResultsIterable<String> iterable;
        private Iterator<String> iterator;
        private String vertex;

        GeNodeValueIndexCursor(QueryResultsIterable<String> results) {
            this.iterable = results;
            this.closed = false;
            this.iterator = Iterables.toSet(this.iterable).iterator();
        }

        @Override
        public void initialize(IndexDescriptor descriptor, IndexQuery[] query, IndexOrder indexOrder, boolean needsValues) {
            this.needsValues = needsValues;
        }

        @Override
        public boolean hasValue() {
            return vertex != null;
        }

        @Override
        public Value propertyValue(String propertyName) {
            NodeValue v = queryContext.getVertexById(vertex, false);
            return (Value) v.properties().get(propertyName);
        }

        @Override
        public String nodeReference() {
            return vertex;
        }

        @Override
        public boolean next() {
            if (iterator.hasNext()) {
                vertex = iterator.next();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void close() {
            this.closed = true;
            if (this.iterable != null) {
                try {
                    this.iterable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public boolean isClosed() {
            return this.closed;
        }

        @Override
        public boolean needsValues() {
            return needsValues;
        }
    }
}
