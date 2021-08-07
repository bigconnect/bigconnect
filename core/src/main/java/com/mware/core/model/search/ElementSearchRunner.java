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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.config.Configuration;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Authorizations;
import com.mware.ge.GeObjectType;
import com.mware.ge.Graph;
import com.mware.ge.query.builder.BoolQueryBuilder;
import org.json.JSONArray;

import java.util.EnumSet;

@Singleton
public class ElementSearchRunner extends GeObjectSearchRunnerWithRelatedBase {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ElementSearchRunner.class);
    public static final String URI = "/element/search";

    @Inject
    public ElementSearchRunner(
            SchemaRepository schemaRepository,
            Graph graph,
            Configuration configuration
    ) {
        super(schemaRepository, graph, configuration);
    }

    @Override
    protected EnumSet<GeObjectType> getResultType() {
        return EnumSet.of(GeObjectType.EDGE, GeObjectType.VERTEX);
    }

    @Override
    public String getUri() {
        return URI;
    }

    @Override
    protected BoolQueryBuilder getQuery(SearchOptions searchOptions, Authorizations authorizations) {
        JSONArray filterJson = getFilterJson(searchOptions, searchOptions.getWorkspaceId());
        BoolQueryBuilder queryBuilder = searchOptions.getRequiredParameter("q", BoolQueryBuilder.class);
        LOGGER.debug("search %s\n%s", queryBuilder, filterJson.toString(2));
        return queryBuilder;
    }
}
