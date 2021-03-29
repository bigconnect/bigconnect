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
 *
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
package com.mware.core.model.search;

import com.mware.ge.GeObject;
import com.mware.ge.query.Query;
import com.mware.ge.query.QueryResultsIterable;

public class QueryResultsIterableSearchResults extends GeObjectsSearchResults implements AutoCloseable {
    private final QueryResultsIterable<? extends GeObject> searchResults;
    private final Query query;
    private final Long offset;
    private final Long size;

    public QueryResultsIterableSearchResults(
            QueryResultsIterable<? extends GeObject> searchResults,
            Query query,
            Long offset,
            Long size
    ) {
        this.searchResults = searchResults;
        this.query = query;
        this.offset = offset;
        this.size = size;
    }

    public QueryResultsIterable<? extends GeObject> getQueryResultsIterable() {
        return searchResults;
    }

    public Query getQuery() {
        return query;
    }

    public Long getOffset() {
        return offset;
    }

    public Long getSize() {
        return size;
    }

    @Override
    public void close() throws Exception {
        this.searchResults.close();
    }

    @Override
    public Iterable<? extends GeObject> getGeObjects() {
        return searchResults;
    }
}
