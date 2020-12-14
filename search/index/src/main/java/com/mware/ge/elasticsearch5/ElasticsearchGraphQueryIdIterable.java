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

import org.elasticsearch.search.SearchHit;
import com.mware.ge.query.aggregations.AggregationResult;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.util.CloseableUtils;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.io.IOException;

public class ElasticsearchGraphQueryIdIterable<T>
        extends ConvertingIterable<SearchHit, T>
        implements QueryResultsIterable<T> {

    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(ElasticsearchGraphQueryIdIterable.class);

    private final IdStrategy idStrategy;
    private final QueryResultsIterable<SearchHit> iterable;

    public ElasticsearchGraphQueryIdIterable(IdStrategy idStrategy, QueryResultsIterable<SearchHit> iterable) {
        super(iterable);
        this.idStrategy = idStrategy;
        this.iterable = iterable;
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(iterable);
    }

    @Override
    public long getTotalHits() {
        return iterable.getTotalHits();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T convert(SearchHit hit) {
         return idFromSearchHit(hit, this.idStrategy);
    }

    @SuppressWarnings("unchecked")
    public static <T> T idFromSearchHit(SearchHit hit, IdStrategy idStrategy) {
        ElasticsearchDocumentType dt = ElasticsearchDocumentType.fromSearchHit(hit);
        T convertedId = null;
        if (dt != null) {
            switch (dt) {
                case VERTEX:
                    convertedId = (T) idStrategy.vertexIdFromSearchHit(hit);
                    break;
                case EDGE:
                    convertedId = (T) idStrategy.edgeIdFromSearchHit(hit);
                    break;
                case VERTEX_EXTENDED_DATA:
                case EDGE_EXTENDED_DATA:
                    convertedId = (T) idStrategy.extendedDataRowIdFromSearchHit(hit);
                    break;
                default:
                    LOGGER.warn("Unhandled document type: %s", dt);
                    break;
            }
        }
        return convertedId;
    }

    @Override
    public <TResult extends AggregationResult> TResult getAggregationResult(String name, Class<? extends TResult> resultType) {
        return iterable.getAggregationResult(name, resultType);
    }
}
