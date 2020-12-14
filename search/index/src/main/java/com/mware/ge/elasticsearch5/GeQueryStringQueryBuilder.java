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

import com.mware.ge.GeException;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import com.mware.ge.Authorizations;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.io.IOException;
import java.util.Objects;

public class GeQueryStringQueryBuilder extends QueryStringQueryBuilder {
    public static final String NAME = "ge_query_string";

    private final Authorizations authorizations;

    private GeQueryStringQueryBuilder(String queryString, Authorizations authorizations) {
        super(queryString);
        this.authorizations = authorizations;
        allowLeadingWildcard(false);
    }

    public static GeQueryStringQueryBuilder build(String queryString, Authorizations authorizations) {
        return new GeQueryStringQueryBuilder(queryString, authorizations);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeStringArray(authorizations.getAuthorizations());
    }

    @Override
    protected void doXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("ge_query_string");
        super.doXContent(builder, params);

        builder.startArray("authorizations");
        for (String authorization : authorizations.getAuthorizations()) {
            builder.value(authorization);
        }
        builder.endArray();

        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        throw new GeException("not implemented");
    }

    @Override
    protected boolean doEquals(QueryStringQueryBuilder other) {
        return other instanceof GeQueryStringQueryBuilder &&
                super.doEquals(other) &&
                Objects.deepEquals(this.authorizations, ((GeQueryStringQueryBuilder)other).authorizations);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode(), authorizations);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
