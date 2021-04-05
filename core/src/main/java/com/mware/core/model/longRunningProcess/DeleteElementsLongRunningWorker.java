package com.mware.core.model.longRunningProcess;

import com.google.inject.Inject;
import com.mware.core.exception.BcException;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.clientapi.dto.ClientApiSearch;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.search.*;
import com.mware.core.model.user.UserRepository;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.*;
import com.mware.ge.tools.GraphBackup;
import org.json.JSONObject;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.mware.core.model.longRunningProcess.DeleteRestoreElementsQueueItem.SEARCH_DELETE_ELEMENTS_TYPE;
import static com.mware.core.model.longRunningProcess.DeleteRestoreUtil.getBackupFileName;

@Name("Delete Elements")
@Description("Delete elements based on a saved search")
public class DeleteElementsLongRunningWorker extends LongRunningProcessWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(DeleteElementsLongRunningWorker.class);

    protected final AuthorizationRepository authorizationRepository;
    protected final LongRunningProcessRepository longRunningProcessRepository;
    protected final UserRepository userRepository;
    protected final GraphBaseWithSearchIndex graph;
    protected final SearchRepository searchRepository;
    protected final SearchHelper searchHelper;

    @Inject
    public DeleteElementsLongRunningWorker(
            AuthorizationRepository authorizationRepository,
            LongRunningProcessRepository longRunningProcessRepository,
            UserRepository userRepository,
            Graph graph,
            SearchRepository searchRepository,
            SearchHelper searchHelper
    ) {
        this.authorizationRepository = authorizationRepository;
        this.longRunningProcessRepository = longRunningProcessRepository;
        this.userRepository = userRepository;
        this.searchRepository = searchRepository;
        this.searchHelper = searchHelper;
        this.graph = (GraphBaseWithSearchIndex) graph;
    }

    @Override
    public boolean isHandled(JSONObject longRunningProcessQueueItem) {
        return longRunningProcessQueueItem.getString("type").equals(SEARCH_DELETE_ELEMENTS_TYPE);
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

        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(user);

        SearchRunner searchRunner = searchRepository.findSearchRunnerByUri(savedSearch.url);
        Map<String, Object> searchParams = savedSearch.prepareParamsForSearch();
        searchParams.put("size", -1);

        long totalhits = 0;
        SearchOptions searchOptions = new SearchOptions(searchParams, SchemaRepository.PUBLIC);
        try (SearchResults searchResults = searchRunner.run(searchOptions, user, authorizations)) {
            QueryResultsIterableSearchResults geObjectsSearchResults = ((QueryResultsIterableSearchResults) searchResults);
            totalhits = geObjectsSearchResults.getQueryResultsIterable().getTotalHits();

            double progress = 0;
            if (totalhits > 0) {
                progress += 0.3;
                longRunningProcessRepository.reportProgress(config, progress, String.format("Deleting item 0 of %d", totalhits));
            } else {
                throw new BcException("Saved search returned no items.");
            }

            String backupFile = "N/A";
            if (deleteElements.isBackup()) {
                backupFile = backupGraphElements(savedSearch, geObjectsSearchResults);
                progress += 0.5;
                longRunningProcessRepository.reportProgress(config, progress, String.format("Finished running backup, to %s file.", backupFile));
            }

            deleteResults(geObjectsSearchResults, authorizations, config, progress, totalhits);

            config.put("backupFile", backupFile);
            config.put("resultsCount", totalhits);
            longRunningProcessRepository.reportProgress(config, 1.0,
                    String.format("Finished running delete for %d items.", totalhits));
        }  catch (Exception e) {
            throw new BcException(String.format("Job with saved search id %s failed. Error msg : %s", savedSearch.id, e.getMessage()), e);
        }
    }

    protected String backupGraphElements(ClientApiSearch savedSearch, QueryResultsIterableSearchResults results) {
        String backupFile = getBackupFileName(savedSearch.name);
        GraphBackup backup = this.graph.getBackupTool(backupFile);
        String absolutePath = backup.getAbsoluteFilePath(backupFile);
        LOGGER.info("Backing up to file: %s, using %s tool", absolutePath, backup.getClass().getName());
        try (OutputStream out = backup.createOutputStream()) {
            for (GeObject geObject : results.getGeObjects()) {
                if (geObject instanceof Vertex) {
                    backup.saveVertex((Vertex) geObject, out);
                } else if (geObject instanceof Edge){
                    backup.saveEdge((Edge) geObject, out);
                }
            }
            return absolutePath;
        } catch (Exception e) {
            LOGGER.error(String.format("Backup failed for file %s", backupFile), e);
            throw new BcException(e.getMessage());
        }
    }

    private void deleteResults(
            QueryResultsIterableSearchResults results,
            Authorizations authorizations,
            JSONObject config,
            double progress,
            long totalHits
    ) {
        double progressUnit = (double)(1 - progress) / totalHits;
        int index = 0;
        for (GeObject geObject : results.getQueryResultsIterable()) {
            try {
                if (geObject instanceof Element) {
                    Element element = (Element) geObject;
                    graph.deleteElement(ElementId.create(element.getElementType(), element.getId()), authorizations);
                    progress += progressUnit;
                }

                longRunningProcessRepository.reportProgress(config, progress,
                        String.format("Deleted %d items of %d", ++index, totalHits));

            } catch (Exception ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new BcException(ex.getMessage());
            }
        }
    }
}
