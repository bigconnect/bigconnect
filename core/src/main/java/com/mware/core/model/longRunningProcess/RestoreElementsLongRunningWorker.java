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
package com.mware.core.model.longRunningProcess;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.exception.BcException;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.ClientApiSearch;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.search.SearchRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.GraphBaseWithSearchIndex;
import com.mware.ge.tools.GraphRestore;
import org.json.JSONObject;

import java.io.InputStream;
import java.util.Optional;

import static com.mware.core.model.longRunningProcess.DeleteRestoreElementsQueueItem.SEARCH_RESTORE_ELEMENTS_TYPE;
import static com.mware.core.model.longRunningProcess.DeleteRestoreUtil.getBackupFilePrefix;

@Name("Restore Elements")
@Description("Restore elements based on a saved search backup")
@Singleton
public class RestoreElementsLongRunningWorker extends LongRunningProcessWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(RestoreElementsLongRunningWorker.class);

    protected final AuthorizationRepository authorizationRepository;
    protected final LongRunningProcessRepository longRunningProcessRepository;
    protected final SearchRepository searchRepository;
    protected final UserRepository userRepository;
    protected final GraphBaseWithSearchIndex graph;

    @Inject
    public RestoreElementsLongRunningWorker(
            AuthorizationRepository authorizationRepository,
            LongRunningProcessRepository longRunningProcessRepository,
            SearchRepository searchRepository,
            UserRepository userRepository,
            Graph graph
    ) {
        this.authorizationRepository = authorizationRepository;
        this.longRunningProcessRepository = longRunningProcessRepository;
        this.searchRepository = searchRepository;
        this.userRepository = userRepository;
        this.graph = (GraphBaseWithSearchIndex) graph;
    }

    @Override
    public boolean isHandled(JSONObject longRunningProcessQueueItem) {
        return longRunningProcessQueueItem.getString("type").equals(SEARCH_RESTORE_ELEMENTS_TYPE);
    }

    @Override
    public void processInternal(final JSONObject config) {
        DeleteRestoreElementsQueueItem deleteElements = ClientApiConverter
                .toClientApi(config.toString(), DeleteRestoreElementsQueueItem.class);
        User user = userRepository.findById(deleteElements.getUserId());
        if (user == null) {
            LOGGER.error(String.format("User with id %s not found.", deleteElements.getUserId()));
            return;
        }
        ClientApiSearch savedSearch = searchRepository.getSavedSearch(deleteElements.getSavedSearchId(), user);
        if (savedSearch == null) {
            LOGGER.error(String.format("Saved search with id %s and name %s not found.",
                        deleteElements.getSavedSearchId(), deleteElements.getSavedSearchName()));
            return;
        }
        LOGGER.info("Start long running restore elements for user: %s, search: %s, uri: %s",
                user.getDisplayName(), savedSearch.id, savedSearch.url);

        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(user);
        LOGGER.debug("Found authorizations: %s", authorizations);

        restoreGraphElements(config, savedSearch, authorizations);
        config.put("restoreComplete", true);

//        report progress
        longRunningProcessRepository.reportProgress(config, 1.0, "Finished successfully.");
    }

    private void restoreGraphElements(JSONObject config, ClientApiSearch savedSearch, Authorizations authorizations) {
        GraphRestore restore = this.graph.getRestoreTool();
        Optional<String> backupFileName = restore.getLastBackupFile(getBackupFilePrefix(savedSearch.name));
        if (!backupFileName.isPresent()) {
            String msg = String.format("Backup file not found for saved search %s.", savedSearch.name);
            LOGGER.error(msg);
            throw new BcException(msg);
        }
        String absolutePath = restore.getAbsoluteFilePath(backupFileName.get());
        LOGGER.info("Restoring from backup file: %s, using %s tool", absolutePath, restore.getClass().getName());
        long results;
        try (InputStream in = restore.createInputStream(backupFileName.get())) {
            results = restore.restore(graph, in, authorizations, 0);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new BcException(e.getMessage());
        }
        config.put("backupFile", absolutePath);
        config.put("resultsCount", results);
    }

}
