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
import com.mware.core.model.clientapi.dto.ClientApiElementSearchResponse;
import com.mware.core.model.clientapi.dto.ClientApiGeObject;
import com.mware.core.model.clientapi.dto.ClientApiSearch;
import com.mware.core.model.clientapi.dto.ClientApiVertex;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SearchHelper {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SearchHelper.class);
    protected final SearchRepository searchRepository;
    protected final GraphBaseWithSearchIndex graph;

    @Inject
    public SearchHelper(
            SearchRepository searchRepository,
            Graph graph
    ) {
        this.searchRepository = searchRepository;
        this.graph = (GraphBaseWithSearchIndex) graph;
    }

    public List<Element> search(ClientApiSearch savedSearch, User user,
                                Authorizations authorizations, boolean includeEdges) {
        SearchRunner searchRunner = searchRepository.findSearchRunnerByUri(savedSearch.url);
        LOGGER.debug("SEARCH - Found search runner: %s", searchRunner.getUri());

        Map<String, Object> searchParams = savedSearch.prepareParamsForSearch();
        List<Element> results = new ArrayList();
        searchParams.put("size", -1);
        SearchOptions searchOptions = new SearchOptions(searchParams, SchemaRepository.PUBLIC);

        try (SearchResults searchResults = searchRunner.run(searchOptions, user, authorizations)) {
            QueryResultsIterableSearchResults geObjectsSearchResults = ((QueryResultsIterableSearchResults) searchResults);
            for (GeObject geo : geObjectsSearchResults.getGeObjects()) {
                if (geo instanceof Vertex || includeEdges) {
                    results.add((Element)geo);
                } else {
                    LOGGER.warn("Can only process instances of Vertex.");
                }
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Job with id %s failed. Error msg : %s", savedSearch.id, e.getMessage()), e);
        }

        return results;
    }

    public List<String> searchCypher(ClientApiSearch savedSearch, User user, Authorizations authorizations) {
        SearchRunner searchRunner = searchRepository.findSearchRunnerByUri(savedSearch.url);
        LOGGER.debug("SEARCH CYPHER - Found search runner: %s", searchRunner.getUri());

        Map<String, Object> searchParams = savedSearch.prepareParamsForSearch();
        List<String> results = new ArrayList();

        SearchOptions searchOptions = new SearchOptions(searchParams, SchemaRepository.PUBLIC);

        try (SearchResults searchResults = searchRunner.run(searchOptions, user, authorizations)) {
            if (searchResults instanceof ClientApiElementSearchResponse) {
                ClientApiElementSearchResponse elementResponse = ((ClientApiElementSearchResponse) searchResults);
                for (ClientApiGeObject geo : elementResponse.getElements()) {
                    if (geo instanceof ClientApiVertex) {
                        results.add(((ClientApiVertex) geo).getId());
                    } else {
                        LOGGER.warn("Can only process instances of Vertex.");
                    }
                }
            } else {
                LOGGER.warn(String.format("Don't know how to process search results of type: %s",
                        searchResults.getClass().getName()));
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Job with id %s failed. Error msg : %s", savedSearch.id, e.getMessage()), e);
        }

        return results;
    }
}
