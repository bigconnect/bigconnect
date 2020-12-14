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

import com.mware.ge.Authorizations;
import com.mware.ge.FetchHints;
import com.mware.ge.Graph;
import com.mware.ge.query.GraphQuery;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.EnumSet;
import java.util.List;

public class ElasticsearchSearchExtendedDataQuery extends ElasticsearchSearchQueryBase implements GraphQuery {
    private final String elementId;
    private final String tableName;

    public ElasticsearchSearchExtendedDataQuery(
            Client client,
            Graph graph,
            String elementId,
            String tableName,
            String queryString,
            Options options,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, options, authorizations);
        this.elementId = elementId;
        this.tableName = tableName;
    }

    @Override
    protected List<QueryBuilder> getFilters(EnumSet<ElasticsearchDocumentType> elementTypes, FetchHints fetchHints) {
        List<QueryBuilder> filters = super.getFilters(elementTypes, fetchHints);
        BoolQueryBuilder filter = QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery(
                        Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME,
                        ElasticsearchDocumentType.VERTEX_EXTENDED_DATA.getKey(),
                        ElasticsearchDocumentType.EDGE_EXTENDED_DATA.getKey()
                ))
                .must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME, elementId));
        if (tableName != null) {
            filter = filter.must(QueryBuilders.termQuery(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME, tableName));
        }
        filters.add(filter);
        return filters;
    }

    @Override
    public String toString() {
        return super.toString() +
                ", elementId=" + elementId +
                ", tableName=" + tableName;
    }
}
