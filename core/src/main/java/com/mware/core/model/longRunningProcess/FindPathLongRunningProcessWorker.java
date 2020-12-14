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
import com.mware.ge.*;
import org.json.JSONObject;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.util.ClientApiConverter;
import com.mware.core.model.clientapi.dto.ClientApiVertexFindPathResponse;

import java.util.ArrayList;
import java.util.List;

@Name("Find Path")
@Description("Finds a path between two vertices")
@Singleton
public class FindPathLongRunningProcessWorker extends LongRunningProcessWorker {
    private final Graph graph;
    private final LongRunningProcessRepository longRunningProcessRepository;

    @Inject
    public FindPathLongRunningProcessWorker(
            Graph graph,
            LongRunningProcessRepository longRunningProcessRepository
    ) {
        this.graph = graph;
        this.longRunningProcessRepository = longRunningProcessRepository;
    }

    @Override
    public boolean isHandled(JSONObject longRunningProcessQueueItem) {
        return longRunningProcessQueueItem.getString("type").equals("findPath");
    }

    @Override
    public void processInternal(final JSONObject longRunningProcessQueueItem) {
        FindPathLongRunningProcessQueueItem findPath = ClientApiConverter.toClientApi(longRunningProcessQueueItem.toString(), FindPathLongRunningProcessQueueItem.class);

        Authorizations authorizations = getAuthorizations(findPath.getAuthorizations());
        String[] labels = findPath.getLabels();
        int hops = findPath.getHops();

        ClientApiVertexFindPathResponse results = new ClientApiVertexFindPathResponse();
        ProgressCallback progressCallback = new ProgressCallback() {
            @Override
            public void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount) {
                longRunningProcessRepository.reportProgress(longRunningProcessQueueItem, progressPercent, step.formatMessage(edgeIndex, vertexCount));
            }
        };

        FindPathOptions findPathOptions = new FindPathOptions(findPath.getOutVertexId(), findPath.getInVertexId(), hops)
                .setLabels(labels)
                .setProgressCallback(progressCallback);
        Iterable<Path> paths = graph.findPaths(findPathOptions, authorizations);
        for (Path path : paths) {
            List<String> clientApiVertexPath = new ArrayList<>();
            for (String s : path) {
                clientApiVertexPath.add(s);
            }
            results.getPaths().add(clientApiVertexPath);
        }

        String resultsString = ClientApiConverter.clientApiToString(results);
        JSONObject resultsJson = new JSONObject(resultsString);
        longRunningProcessQueueItem.put("results", resultsJson);
        longRunningProcessQueueItem.put("resultsCount", results.getPaths().size());
    }

    private Authorizations getAuthorizations(String[] authorizations) {
        return graph.createAuthorizations(authorizations);
    }
}
