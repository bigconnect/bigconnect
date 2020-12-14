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
package com.mware.core.model.longRunningProcess;


import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.exception.BcException;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.*;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

@Name("Reindex")
@Description("Reindexes the specified elements")
@Singleton
public class ReindexLongRunningProcessWorker extends LongRunningProcessWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ReindexLongRunningProcessWorker.class);
    private static final FetchHints FETCH_HINTS = FetchHints.ALL;
    private final Authorizations authorizations;
    private final Graph graph;

    @Inject
    public ReindexLongRunningProcessWorker(
            Graph graph,
            UserRepository userRepository,
            AuthorizationRepository authorizationRepository
    ) {
        this.graph = graph;
        this.authorizations = authorizationRepository.getGraphAuthorizations(userRepository.getSystemUser());
    }

    @Override
    public boolean isHandled(JSONObject jsonObject) {
        return ReindexLongRunningProcessQueueItem.isHandled(jsonObject);
    }

    @Override
    protected void processInternal(JSONObject longRunningProcessQueueItem) {
        ReindexLongRunningProcessQueueItem queueItem = ClientApiConverter.toClientApi(
                longRunningProcessQueueItem.toString(),
                ReindexLongRunningProcessQueueItem.class
        );
        int batchSize = queueItem.getBatchSize();
        IdRange range = new IdRange(queueItem.getStartId(), queueItem.getEndId());
        LOGGER.info("reindex %s %s", range, queueItem.getElementType());
        if (queueItem.getElementType() == ElementType.VERTEX) {
            reindexVertices(range, batchSize, authorizations);
        } else if (queueItem.getElementType() == ElementType.EDGE) {
            reindexEdges(range, batchSize, authorizations);
        } else {
            throw new BcException("Unhandled element type: " + queueItem.getElementType());
        }
    }

    public void reindexVertices(IdRange range, int batchSize, Authorizations authorizations) {
        Iterable<Vertex> vertices = graph.getVerticesInRange(range, FETCH_HINTS, authorizations);
        reindexElements(vertices, batchSize, authorizations);
    }

    public void reindexEdges(IdRange range, int batchSize, Authorizations authorizations) {
        Iterable<Edge> edges = graph.getEdgesInRange(range, FETCH_HINTS, authorizations);
        reindexElements(edges, batchSize, authorizations);
    }

    private void reindexElements(
            Iterable<? extends Element> elements,
            int batchSize,
            Authorizations authorizations
    ) {
        List<Element> batch = new ArrayList<>(batchSize);
        for (Element element : elements) {
            batch.add(element);
            if (batch.size() == batchSize) {
                ((GraphWithSearchIndex) graph).getSearchIndex().addElements(graph, batch, authorizations);
                batch.clear();
            }
        }
        if (batch.size() > 0) {
            ((GraphWithSearchIndex) graph).getSearchIndex().addElements(graph, batch, authorizations);
            batch.clear();
        }
    }
}
