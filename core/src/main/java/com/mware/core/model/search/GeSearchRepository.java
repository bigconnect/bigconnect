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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcAccessDeniedException;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.ClientApiSearch;
import com.mware.core.model.clientapi.dto.ClientApiSearchListResponse;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.graph.GraphUpdateContext;
import com.mware.core.model.properties.SearchSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.*;
import org.json.JSONObject;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.core.util.StreamUtil.stream;

@Singleton
public class GeSearchRepository extends SearchRepository {
    public static final String VISIBILITY_STRING = "search";
    public static final BcVisibility VISIBILITY = new BcVisibility(VISIBILITY_STRING);
    private static final String GLOBAL_SAVED_SEARCHES_ROOT_VERTEX_ID = "__bc_globalSavedSearchesRoot";
    private final Graph graph;
    private final GraphRepository graphRepository;
    private final UserRepository userRepository;
    private final AuthorizationRepository authorizationRepository;
    private final PrivilegeRepository privilegeRepository;
    private final WorkspaceRepository workspaceRepository;

    @Inject
    public GeSearchRepository(
            Graph graph,
            GraphRepository graphRepository,
            UserRepository userRepository,
            Configuration configuration,
            GraphAuthorizationRepository graphAuthorizationRepository,
            AuthorizationRepository authorizationRepository,
            PrivilegeRepository privilegeRepository,
            WorkspaceRepository workspaceRepository
    ) {
        super(configuration);
        this.graph = graph;
        this.graphRepository = graphRepository;
        this.userRepository = userRepository;
        this.authorizationRepository = authorizationRepository;
        this.privilegeRepository = privilegeRepository;
        this.workspaceRepository = workspaceRepository;
        graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);
    }

    @Override
    public String saveSearch(
            String id,
            String name,
            String url,
            JSONObject searchParameters,
            User user,
            boolean update
    ) {
        checkNotNull(user, "User is required");
        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING
        );

        if (!update && savedSearchWithSameNameExists(name, getUserSavedSearches(user, authorizations))) {
            throw new BcException("An existing saved search with the same name already exists.");
        }

        if (graph.doesVertexExist(id, authorizations)) {
            // switching from global to private
            if (isSearchGlobal(id, authorizations)) {
                if (privilegeRepository.hasPrivilege(user, Privilege.SEARCH_SAVE_GLOBAL)) {
                    deleteSearch(id, user);
                } else {
                    throw new BcAccessDeniedException(
                            "User does not have the privilege to change a global search", user, id);
                }
            } else if (!isSearchPrivateToUser(id, user, authorizations)) {
                throw new BcAccessDeniedException("User does not own this this search", user, id);
            }
        }

        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.LOW, user, authorizations)) {
            ctx.setPushOnQueue(false);
            Vertex searchVertex = saveSearchVertex(ctx, id, name, url, searchParameters, user).get();

            Vertex userVertex = graph.getVertex(user.getUserId(), authorizations);
            checkNotNull(userVertex, "Could not find user vertex with id " + user.getUserId());
            String edgeId = userVertex.getId() + "_" + SearchSchema.HAS_SAVED_SEARCH + "_" + searchVertex.getId();
            ctx.getOrCreateEdgeAndUpdate(
                    edgeId,
                    userVertex.getId(),
                    searchVertex.getId(),
                    SearchSchema.HAS_SAVED_SEARCH,
                    VISIBILITY.getVisibility(),
                    elemCtx -> {
                    }
            );

            return searchVertex.getId();
        } catch (Exception ex) {
            throw new BcException("Could not save private search", ex);
        }
    }

    @Override
    public String saveGlobalSearch(
            String id,
            String name,
            String url,
            JSONObject searchParameters,
            User user,
            boolean update
    ) {
        if (!(user instanceof SystemUser) && !privilegeRepository.hasPrivilege(user, Privilege.SEARCH_SAVE_GLOBAL)) {
            throw new BcAccessDeniedException(
                    "User does not have the privilege to save a global search", user, id);
        }

        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(
                userRepository.getSystemUser(),
                VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING
        );

        if (!update && savedSearchWithSameNameExists(name, getGlobalSavedSearches(authorizations))) {
            throw new BcException("An existing global saved search with the same name already exists.");
        }

        // switching from private to global
        if (isSearchPrivateToUser(id, user, authorizations)) {
            deleteSearch(id, user);
        }

        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.LOW, user, authorizations)) {
            ctx.setPushOnQueue(false);
            Vertex searchVertex = saveSearchVertex(ctx, id, name, url, searchParameters, user).get();

            String edgeId = String.format(
                    "%s_%s_%s",
                    GLOBAL_SAVED_SEARCHES_ROOT_VERTEX_ID, SearchSchema.HAS_SAVED_SEARCH, searchVertex.getId()
            );
            ctx.getOrCreateEdgeAndUpdate(
                    edgeId,
                    getGlobalSavedSearchesRootVertex().getId(),
                    searchVertex.getId(),
                    SearchSchema.HAS_SAVED_SEARCH,
                    VISIBILITY.getVisibility(),
                    elemCtx -> {
                    }
            );

            return searchVertex.getId();
        } catch (Exception ex) {
            throw new BcException("Could not save global search", ex);
        }
    }

    private boolean savedSearchWithSameNameExists(String name, Iterable<ClientApiSearch> searches) {
        return StreamSupport.stream(searches.spliterator(), false).anyMatch(ss -> ss.name.equals(name));
    }

    private GraphUpdateContext.UpdateFuture<Vertex> saveSearchVertex(
            GraphUpdateContext ctx,
            String id,
            String name,
            String url,
            JSONObject searchParameters,
            User user
    ) {
        Visibility visibility = VISIBILITY.getVisibility();
        return ctx.getOrCreateVertexAndUpdate(id, visibility, SearchSchema.CONCEPT_TYPE_SAVED_SEARCH, elemCtx -> {
            PropertyMetadata metadata = new PropertyMetadata(user, new VisibilityJson(), visibility);
            if (elemCtx.isNewElement()) {
                elemCtx.updateBuiltInProperties(metadata);
            }
            SearchSchema.NAME.updateProperty(elemCtx, name != null ? name : "", metadata);
            SearchSchema.URL.updateProperty(elemCtx, url, metadata);
            SearchSchema.PARAMETERS.updateProperty(elemCtx, searchParameters, metadata);
        });
    }

    @Override
    public ClientApiSearchListResponse getSavedSearches(User user) {
        checkNotNull(user, "User is required");
        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING
        );

        ClientApiSearchListResponse result = new ClientApiSearchListResponse();
        Iterables.addAll(result.searches, getGlobalSavedSearches(authorizations));
        Iterables.addAll(result.searches, getUserSavedSearches(user, authorizations));
        return result;
    }

    private Iterable<ClientApiSearch> getUserSavedSearches(User user, Authorizations authorizations) {
        Vertex userVertex = graph.getVertex(user.getUserId(), authorizations);
        checkNotNull(userVertex, "Could not find user vertex with id " + user.getUserId());
        Iterable<Vertex> userSearchVertices = userVertex.getVertices(
                Direction.OUT,
                SearchSchema.HAS_SAVED_SEARCH,
                authorizations
        );
        return stream(userSearchVertices)
                .map(searchVertex -> toClientApiSearch(searchVertex, ClientApiSearch.Scope.User))
                .collect(Collectors.toList());
    }

    private Iterable<ClientApiSearch> getGlobalSavedSearches(Authorizations authorizations) {
        Vertex globalSavedSearchesRootVertex = getGlobalSavedSearchesRootVertex();
        Iterable<Vertex> globalSearchVertices = globalSavedSearchesRootVertex.getVertices(
                Direction.OUT,
                SearchSchema.HAS_SAVED_SEARCH,
                authorizations
        );
        return stream(globalSearchVertices)
                .map(searchVertex -> toClientApiSearch(searchVertex, ClientApiSearch.Scope.Global))
                .collect(Collectors.toList());
    }

    private Vertex getGlobalSavedSearchesRootVertex() {
        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(
                userRepository.getSystemUser(),
                VISIBILITY_STRING
        );
        Vertex globalSavedSearchesRootVertex = graph.getVertex(GLOBAL_SAVED_SEARCHES_ROOT_VERTEX_ID, authorizations);
        if (globalSavedSearchesRootVertex == null) {
            globalSavedSearchesRootVertex = graph.prepareVertex(
                    GLOBAL_SAVED_SEARCHES_ROOT_VERTEX_ID,
                    new Visibility(VISIBILITY_STRING),
                    SchemaConstants.CONCEPT_TYPE_THING
            )
                    .save(authorizations);
            graph.flush();
        }
        return globalSavedSearchesRootVertex;
    }

    private static ClientApiSearch toClientApiSearch(Vertex searchVertex) {
        return toClientApiSearch(searchVertex, null);
    }

    public static ClientApiSearch toClientApiSearch(Vertex searchVertex, ClientApiSearch.Scope scope) {
        ClientApiSearch result = new ClientApiSearch();
        result.id = searchVertex.getId();
        result.name = SearchSchema.NAME.getPropertyValue(searchVertex);
        result.url = SearchSchema.URL.getPropertyValue(searchVertex);
        result.scope = scope;
        result.parameters = ClientApiConverter.toClientApiValue(SearchSchema.PARAMETERS.getPropertyValue(searchVertex));
        return result;
    }

    @Override
    public ClientApiSearch getSavedSearch(String id, User user) {
        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING
        );
        Vertex searchVertex = graph.getVertex(id, authorizations);
        if (searchVertex == null) {
            return null;
        }
        return toClientApiSearch(searchVertex);
    }

    @Override
    public ClientApiSearch getSavedSearchOnWorkspace(String id, User user, String workspaceId) {
        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING
        );

        Vertex searchVertex = graph.getVertex(id, authorizations);
        if (searchVertex == null) {
            return null;
        }

        boolean isGlobalSearch = isSearchGlobal(id, authorizations);
        boolean hasWorkspaceAccess = workspaceId != null && workspaceRepository.hasReadPermissions(workspaceId, user);

        if (isGlobalSearch /*|| isSearchPrivateToUser(id, user, authorizations)*/) {
            return toClientApiSearch(searchVertex);
        } else if (!isGlobalSearch && !hasWorkspaceAccess) {
            return null;
        } else {
            String workspaceCreatorId = workspaceRepository.getCreatorUserId(workspaceId, user);
            if (isSearchPrivateToUser(id, userRepository.findById(workspaceCreatorId), authorizations)) {
                return toClientApiSearch(searchVertex);
            }
            return null;
        }
    }

    @Override
    public void deleteSearch(final String id, User user) {
        checkNotNull(user, "User is required");
        Authorizations authorizations = authorizationRepository.getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING
        );
        Vertex searchVertex = graph.getVertex(id, authorizations);
        checkNotNull(searchVertex, "Could not find search with id " + id);

        if (isSearchGlobal(id, authorizations)) {
            if (!privilegeRepository.hasPrivilege(user, Privilege.SEARCH_SAVE_GLOBAL)) {
                throw new BcAccessDeniedException(
                        "User does not have the privilege to delete a global search", user, id);
            }
        } else if (!isSearchPrivateToUser(id, user, authorizations)) {
            throw new BcAccessDeniedException("User does not own this this search", user, id);
        }

        graph.deleteVertex(searchVertex, authorizations);
        graph.flush();
    }

    @VisibleForTesting
    boolean isSearchGlobal(String id, Authorizations authorizations) {
        if (!graph.doesVertexExist(id, authorizations)) {
            return false;
        }
        Iterable<String> vertexIds = getGlobalSavedSearchesRootVertex().getVertexIds(
                Direction.OUT,
                SearchSchema.HAS_SAVED_SEARCH,
                authorizations
        );
        return stream(vertexIds).anyMatch(vertexId -> vertexId.equals(id));
    }

    @VisibleForTesting
    boolean isSearchPrivateToUser(String id, User user, Authorizations authorizations) {
        if (user instanceof SystemUser) {
            return false;
        }
        Vertex userVertex = graph.getVertex(user.getUserId(), authorizations);
        checkNotNull(userVertex, "Could not find user vertex with id " + user.getUserId());
        Iterable<String> vertexIds = userVertex.getVertexIds(
                Direction.OUT,
                SearchSchema.HAS_SAVED_SEARCH,
                authorizations
        );
        return stream(vertexIds).anyMatch(vertexId -> vertexId.equals(id));
    }
}
