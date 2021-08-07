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
package com.mware.core.model.workspace;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcAccessDeniedException;
import com.mware.core.exception.BcResourceNotFoundException;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.clientapi.dto.WorkspaceAccess;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.properties.WorkspaceSchema;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.GeUserRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.trace.Traced;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingEdgeMutation;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.search.IndexHint;
import com.mware.ge.util.FilterIterable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.core.util.StreamUtil.stream;

@Singleton
public class GeWorkspaceRepository extends WorkspaceRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(GeWorkspaceRepository.class);
    private final UserRepository userRepository;
    private final GraphAuthorizationRepository graphAuthorizationRepository;
    private final LockRepository lockRepository;
    private Cache<String, Boolean> usersWithReadAccessCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();
    private Cache<String, Boolean> usersWithCommentAccessCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();
    private Cache<String, Boolean> usersWithWriteAccessCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();
    private Cache<String, List<WorkspaceUser>> usersWithAccessCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();
    private Cache<String, Vertex> userWorkspaceVertexCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();

    @Override
    public void clearCache() {
        usersWithReadAccessCache.invalidateAll();
        usersWithCommentAccessCache.invalidateAll();
        usersWithWriteAccessCache.invalidateAll();
        usersWithAccessCache.invalidateAll();
        userWorkspaceVertexCache.invalidateAll();
    }

    @Inject
    public GeWorkspaceRepository(
            Graph graph,
            Configuration configuration,
            UserRepository userRepository,
            GraphAuthorizationRepository graphAuthorizationRepository,
            LockRepository lockRepository,
            VisibilityTranslator visibilityTranslator,
            TermMentionRepository termMentionRepository,
            SchemaRepository schemaRepository,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            AuthorizationRepository authorizationRepository
    ) {
        super(
                graph,
                configuration,
                visibilityTranslator,
                termMentionRepository,
                schemaRepository,
                workQueueRepository,
                webQueueRepository,
                authorizationRepository
        );
        this.userRepository = userRepository;
        this.graphAuthorizationRepository = graphAuthorizationRepository;
        this.lockRepository = lockRepository;

        graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);
        graphAuthorizationRepository.addAuthorizationToGraph(BcVisibility.SUPER_USER_VISIBILITY_STRING);

        this.userRepository.addDefaultAdminUser();
        this.userRepository.addDefaultSysUser();
    }

    @Override
    public void delete(final Workspace workspace, final User user) {
        if (!hasWritePermissions(workspace.getWorkspaceId(), user)) {
            throw new BcAccessDeniedException(
                    "user " + user.getUserId() + " does not have write access to workspace " + workspace.getWorkspaceId(),
                    user,
                    workspace.getWorkspaceId()
            );
        }

        fireWorkspaceBeforeDelete(workspace, user);

        lockRepository.lock(getLockName(workspace), () -> {
            Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                    user,
                    UserRepository.VISIBILITY_STRING,
                    BcVisibility.SUPER_USER_VISIBILITY_STRING,
                    workspace.getWorkspaceId()
            );
            Vertex workspaceVertex = getVertexFromWorkspace(workspace, true, authorizations);

            // Delete all sandboxed entities
            Iterable<EdgeInfo> edgeInfos = workspaceVertex.getEdgeInfos(Direction.OUT, WorkspaceSchema.WORKSPACE_TO_ENTITY_RELATIONSHIP_NAME, authorizations);
            edgeInfos.forEach(edgeInfo -> {
                getGraph().softDeleteEdge(edgeInfo.getEdgeId(), authorizations);
            });

            // Clean up all sandboxed vertices
            List<String> connectedVertexIds = stream(edgeInfos)
                    .map(edgeInfo -> edgeInfo.getVertexId())
                    .collect(Collectors.toList());

            Iterable<Vertex> vertices = getGraph().getVertices(connectedVertexIds, authorizations);
            vertices.forEach(v -> {
                SandboxStatus sandboxStatus = SandboxStatus.getFromVisibilityString(v.getVisibility().getVisibilityString(), workspace.getWorkspaceId());
                if (sandboxStatus == SandboxStatus.PRIVATE) {
                    getGraph().softDeleteVertex(v, authorizations);
                }
            });

            getGraph().softDeleteVertex(workspaceVertex, authorizations);

            List<WorkspaceUser> usersWithAccess = findUsersWithAccess(workspace.getWorkspaceId(), user);
            usersWithAccess.forEach(userWithAccess -> {
                if (workspace.getWorkspaceId().equals(userRepository.getCurrentWorkspaceId(userWithAccess.getUserId()))) {
                    userRepository.setCurrentWorkspace(userWithAccess.getUserId(), null);
                }
            });

            getGraph().flush();
            clearCache();

            graphAuthorizationRepository.removeAuthorizationFromGraph(workspace.getWorkspaceId());
        });
    }

    public Vertex getVertex(String workspaceId, User user) {
        String cacheKey = getUserWorkspaceVertexCacheKey(workspaceId, user);
        Vertex workspaceVertex = userWorkspaceVertexCache.getIfPresent(cacheKey);
        if (workspaceVertex != null) {
            return workspaceVertex;
        }

        Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                user,
                UserRepository.VISIBILITY_STRING,
                BcVisibility.SUPER_USER_VISIBILITY_STRING,
                workspaceId
        );
        workspaceVertex = getGraph().getVertex(workspaceId, FetchHints.ALL, authorizations);
        if (workspaceVertex != null) {
            userWorkspaceVertexCache.put(cacheKey, workspaceVertex);
        }
        return workspaceVertex;
    }

    public String getUserWorkspaceVertexCacheKey(String workspaceId, User user) {
        return workspaceId + user.getUserId();
    }

    private Vertex getVertexFromWorkspace(Workspace workspace, boolean includeHidden, Authorizations authorizations) {
        if (workspace instanceof GeWorkspace) {
            return ((GeWorkspace) workspace).getVertex(getGraph(), includeHidden, authorizations);
        }
        return getGraph().getVertex(
                workspace.getWorkspaceId(),
                includeHidden ? FetchHints.ALL_INCLUDING_HIDDEN : FetchHints.ALL,
                authorizations
        );
    }

    @Override
    @Traced
    public Workspace findById(String workspaceId, boolean includeHidden, User user) {
        LOGGER.debug("findById(workspaceId: %s, userId: %s)", workspaceId, user.getUserId());
        Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                workspaceId
        );

        Vertex workspaceVertex;
        try {
            workspaceVertex = getGraph().getVertex(
                    workspaceId,
                    includeHidden ? FetchHints.ALL_INCLUDING_HIDDEN : FetchHints.ALL,
                    authorizations
            );
        } catch (SecurityGeException e) {
            if (!graphAuthorizationRepository.getGraphAuthorizations().contains(workspaceId)) {
                return null;
            }

            String message = String.format("user %s does not have read access to workspace %s", user.getUserId(), workspaceId);
            LOGGER.warn("%s", message, e);
            throw new BcAccessDeniedException(
                    message,
                    user,
                    workspaceId
            );
        }

        if (workspaceVertex == null) {
            return null;
        }

        if (!hasReadPermissions(workspaceId, user)) {
            throw new BcAccessDeniedException(
                    "user " + user.getUserId() + " does not have read access to workspace " + workspaceId,
                    user,
                    workspaceId
            );
        }
        return new GeWorkspace(workspaceVertex);
    }

    @Override
    public Workspace add(String workspaceId, String title, User user) {
        if (workspaceId == null) {
            workspaceId = WORKSPACE_ID_PREFIX + getGraph().getIdGenerator().nextId();
        }

        graphAuthorizationRepository.addAuthorizationToGraph(workspaceId);

        Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                user,
                UserRepository.VISIBILITY_STRING,
                VISIBILITY_STRING,
                workspaceId
        );
        Vertex userVertex = null;
        if (!user.getUserId().equals(userRepository.getSystemUser().getUserId())) {
            userVertex = getGraph().getVertex(user.getUserId(), authorizations);
            checkNotNull(userVertex, "Could not find user: " + user.getUserId());
        }

        VertexBuilder workspaceVertexBuilder = getGraph().prepareVertex(workspaceId, VISIBILITY.getVisibility(), WORKSPACE_CONCEPT_NAME);
        WorkspaceSchema.TITLE.setProperty(workspaceVertexBuilder, title, VISIBILITY.getVisibility());
        Vertex workspaceVertex = workspaceVertexBuilder.save(authorizations);

        if (userVertex != null) {
            addWorkspaceToUser(workspaceVertex, userVertex, authorizations);
        }

        getGraph().flush();

        GeWorkspace workspace = new GeWorkspace(workspaceVertex);
        fireWorkspaceAdded(workspace, user);
        return workspace;
    }

    public void addWorkspaceToUser(Vertex workspaceVertex, Vertex userVertex, Authorizations authorizations) {
        EdgeBuilder edgeBuilder = getGraph().prepareEdge(
                workspaceVertex,
                userVertex,
                WORKSPACE_TO_USER_RELATIONSHIP_NAME,
                VISIBILITY.getVisibility()
        );
        WorkspaceSchema.WORKSPACE_TO_USER_IS_CREATOR.setProperty(edgeBuilder, true, VISIBILITY.getVisibility());
        WorkspaceSchema.WORKSPACE_TO_USER_ACCESS.setProperty(
                edgeBuilder,
                WorkspaceAccess.WRITE.toString(),
                VISIBILITY.getVisibility()
        );
        edgeBuilder.save(authorizations);
    }

    @Override
    public Iterable<Workspace> findAllForUser(final User user) {
        checkNotNull(user, "User is required");
        Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING
        );
        Vertex userVertex = getGraph().getVertex(user.getUserId(), FetchHints.ALL, authorizations);
        checkNotNull(userVertex, "Could not find user vertex with id " + user.getUserId());
        return stream(userVertex.getVertices(Direction.IN, WORKSPACE_TO_USER_RELATIONSHIP_NAME, FetchHints.ALL, authorizations))
                .map((Vertex workspaceVertex) -> {
                    String cacheKey = getUserWorkspaceVertexCacheKey(workspaceVertex.getId(), user);
                    userWorkspaceVertexCache.put(cacheKey, workspaceVertex);
                    return new GeWorkspace(workspaceVertex);
                })
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<Workspace> findAll(User user) {
        if (!user.equals(userRepository.getSystemUser())) {
            throw new BcAccessDeniedException("Only system user can access all workspaces", user, null);
        }
        Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                UserRepository.VISIBILITY_STRING
        );
        try (QueryResultsIterable<Vertex> workspaceVertices =
                    getGraph().query(GeQueryBuilders.hasConceptType(WORKSPACE_CONCEPT_NAME), authorizations).vertices()) {
            return stream(workspaceVertices)
                    .map((Vertex workspaceVertex) -> {
                        String cacheKey = getUserWorkspaceVertexCacheKey(workspaceVertex.getId(), user);
                        userWorkspaceVertexCache.put(cacheKey, workspaceVertex);
                        return new GeWorkspace(workspaceVertex);
                    })
                    .collect(Collectors.toList());
        } catch(Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public void setTitle(Workspace workspace, String title, User user) {
        if (!hasWritePermissions(workspace.getWorkspaceId(), user)) {
            throw new BcAccessDeniedException(
                    "user " + user.getUserId() + " does not have write access to workspace " + workspace.getWorkspaceId(),
                    user,
                    workspace.getWorkspaceId()
            );
        }
        Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(user);
        Vertex workspaceVertex = getVertexFromWorkspace(workspace, false, authorizations);
        WorkspaceSchema.TITLE.setProperty(workspaceVertex, title, VISIBILITY.getVisibility(), authorizations);
        getGraph().flush();
    }

    @Override
    @Traced
    public List<WorkspaceUser> findUsersWithAccess(final String workspaceId, final User user) {
        String cacheKey = workspaceId + user.getUserId();
        List<WorkspaceUser> usersWithAccess = this.usersWithAccessCache.getIfPresent(cacheKey);
        if (usersWithAccess != null) {
            return usersWithAccess;
        }

        LOGGER.debug("BEGIN findUsersWithAccess query");
        Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                workspaceId
        );

        Vertex workspaceVertex = getVertex(workspaceId, user);
        if (workspaceVertex == null) {
            return Lists.newArrayList();
        } else {
            Iterable<Edge> userEdges = workspaceVertex.getEdges(
                    Direction.OUT,
                    WORKSPACE_TO_USER_RELATIONSHIP_NAME,
                    authorizations
            );
            usersWithAccess = stream(userEdges)
                    .map((edge) -> {
                        String userId = edge.getOtherVertexId(workspaceId);

                        String accessString = WorkspaceSchema.WORKSPACE_TO_USER_ACCESS.getPropertyValue(edge);
                        WorkspaceAccess workspaceAccess = WorkspaceAccess.NONE;
                        if (accessString != null && accessString.length() > 0) {
                            workspaceAccess = WorkspaceAccess.valueOf(accessString);
                        }

                        boolean isCreator = WorkspaceSchema.WORKSPACE_TO_USER_IS_CREATOR.getPropertyValue(edge, false);

                        return new WorkspaceUser(userId, workspaceAccess, isCreator);
                    })
                    .collect(Collectors.toList());

            this.usersWithAccessCache.put(cacheKey, usersWithAccess);
            LOGGER.debug("END findUsersWithAccess query");
            return usersWithAccess;
        }
    }

    @Override
    public List<WorkspaceEntity> findEntities(final Workspace workspace, final boolean fetchVertices, final User user, boolean lock, boolean hidden) {
        if (!hasReadPermissions(workspace.getWorkspaceId(), user)) {
            throw new BcAccessDeniedException(
                    "user " + user.getUserId() + " does not have read access to workspace " + workspace.getWorkspaceId(),
                    user,
                    workspace.getWorkspaceId()
            );
        }

        if(lock) {
            return lockRepository.lock(
                    getLockName(workspace),
                    () -> findEntitiesNoLock(workspace, hidden, fetchVertices, user)
            );
        } else {
            return findEntitiesNoLock(workspace, hidden, fetchVertices, user);
        }
    }

    @Traced
    public List<WorkspaceEntity> findEntitiesNoLock(
            final Workspace workspace,
            final boolean includeHidden,
            final boolean fetchVertices,
            User user
    ) {
        LOGGER.debug(
                "BEGIN findEntitiesNoLock(workspaceId: %s, includeHidden: %b, userId: %s)",
                workspace.getWorkspaceId(),
                includeHidden,
                user.getUserId()
        );
        long startTime = System.currentTimeMillis();
        Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                workspace.getWorkspaceId()
        );
        Vertex workspaceVertex = getVertexFromWorkspace(workspace, includeHidden, authorizations);
        List<Edge> entityEdges = stream(workspaceVertex.getEdges(
                Direction.BOTH,
                WORKSPACE_TO_ENTITY_RELATIONSHIP_NAME,
                includeHidden ? FetchHints.ALL_INCLUDING_HIDDEN : FetchHints.ALL,
                authorizations
        ))
                .collect(Collectors.toList());

        final Map<String, Vertex> workspaceVertices;
        if (fetchVertices) {
            workspaceVertices = getWorkspaceVertices(workspace, entityEdges, authorizations);
        } else {
            workspaceVertices = null;
        }

        List<WorkspaceEntity> results = entityEdges.stream()
                .map(edge -> {
                    String entityVertexId = edge.getOtherVertexId(workspace.getWorkspaceId());

                    if (!includeHidden) {
                        return null;
                    }

                    Vertex workspaceVertex1 = null;
                    if (fetchVertices) {
                        workspaceVertex1 = workspaceVertices.get(entityVertexId);
                    }
                    return new WorkspaceEntity(
                            entityVertexId,
                            workspaceVertex1
                    );
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        LOGGER.debug(
                "END findEntitiesNoLock (found: %d entities, time: %dms)",
                results.size(),
                System.currentTimeMillis() - startTime
        );
        return results;
    }

    protected Map<String, Vertex> getWorkspaceVertices(
            final Workspace workspace,
            List<Edge> entityEdges,
            Authorizations authorizations
    ) {
        Map<String, Vertex> workspaceVertices;
        Iterable<String> workspaceVertexIds = entityEdges.stream()
                .map(edge -> edge.getOtherVertexId(workspace.getWorkspaceId()))
                .collect(Collectors.toList());
        Iterable<Vertex> vertices = getGraph().getVertices(
                workspaceVertexIds,
                FetchHints.ALL_INCLUDING_HIDDEN,
                authorizations
        );
        workspaceVertices = Maps.uniqueIndex(vertices, new Function<Vertex, String>() {
            @Override
            public String apply(Vertex v) {
                return v.getId();
            }
        });
        return workspaceVertices;
    }

    @Override
    public Workspace copyTo(Workspace workspace, User destinationUser, User user) {
        Workspace newWorkspace = super.copyTo(workspace, destinationUser, user);
        getGraph().flush();
        return newWorkspace;
    }

    @Override
    public void softDeleteEntitiesFromWorkspace(Workspace workspace, List<String> entityIdsToDelete, User user) {
        if (entityIdsToDelete.size() == 0) {
            return;
        }
        if (!hasWritePermissions(workspace.getWorkspaceId(), user)) {
            throw new BcAccessDeniedException(
                    "user " + user.getUserId() + " does not have write access to workspace " + workspace.getWorkspaceId(),
                    user,
                    workspace.getWorkspaceId()
            );
        }
        Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                user,
                VISIBILITY_STRING,
                workspace.getWorkspaceId()
        );
        final Vertex workspaceVertex = getVertexFromWorkspace(workspace, true, authorizations);
        List<Edge> allEdges = stream(workspaceVertex.getEdges(
                Direction.BOTH,
                authorizations
        )).collect(Collectors.toList());

        for (final String vertexId : entityIdsToDelete) {
            LOGGER.debug("workspace delete (%s): %s", workspace.getWorkspaceId(), vertexId);

            Iterable<Edge> edges = new FilterIterable<Edge>(allEdges) {
                @Override
                protected boolean isIncluded(Edge o) {
                    String entityVertexId = o.getOtherVertexId(workspaceVertex.getId());
                    return entityVertexId.equalsIgnoreCase(vertexId);
                }
            };
            for (Edge edge : edges) {
                ExistingEdgeMutation m = edge.prepareMutation();
                m.setIndexHint(IndexHint.DO_NOT_INDEX);
                m.save(authorizations);
            }
        }
        getGraph().flush();
    }

    @Override
    public void updateEntitiesOnWorkspace(
            final Workspace workspace,
            final Collection<String> vertexIds,
            final User user
    ) {
        if (vertexIds.size() == 0) {
            return;
        }
        if (!hasCommentPermissions(workspace.getWorkspaceId(), user)) {
            throw new BcAccessDeniedException(
                    "user " + user.getUserId() + " does not have comment access to workspace " + workspace.getWorkspaceId(),
                    user,
                    workspace.getWorkspaceId()
            );
        }

        lockRepository.lock(getLockName(workspace.getWorkspaceId()), () -> {
            Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                    user,
                    VISIBILITY_STRING,
                    VISIBILITY_PRODUCT_STRING,
                    workspace.getWorkspaceId()
            );

            Vertex workspaceVertex = getVertexFromWorkspace(workspace, true, authorizations);
            if (workspaceVertex == null) {
                throw new BcResourceNotFoundException(
                        "Could not find workspace vertex: " + workspace.getWorkspaceId(),
                        workspace.getWorkspaceId()
                );
            }

            Iterable<Vertex> vertices = getGraph().getVertices(vertexIds, authorizations);
            ImmutableMap<String, Vertex> verticesMap = Maps.uniqueIndex(vertices, Element::getId);

            for (String vertexId : vertexIds) {
                Vertex otherVertex = verticesMap.get(vertexId);
                if (otherVertex == null) {
                    LOGGER.error(
                            "updateEntitiesOnWorkspace: could not find vertex with id \"%s\" for workspace \"%s\"",
                            vertexId,
                            workspace.getWorkspaceId()
                    );
                    continue;
                }
                createEdge(
                        workspaceVertex,
                        otherVertex,
                        authorizations
                );
            }
            getGraph().flush();
        });

        fireWorkspaceUpdateEntities(workspace, vertexIds, user);
    }

    private void createEdge(
            Vertex workspaceVertex,
            Vertex otherVertex,
            Authorizations authorizations
    ) {
        String workspaceVertexId = workspaceVertex.getId();
        String entityVertexId = otherVertex.getId();
        String edgeId = getWorkspaceToEntityEdgeId(workspaceVertexId, entityVertexId);
        EdgeBuilder edgeBuilder = getGraph().prepareEdge(
                edgeId,
                workspaceVertex,
                otherVertex,
                WORKSPACE_TO_ENTITY_RELATIONSHIP_NAME,
                VISIBILITY.orVisibility(workspaceVertexId)
        );
        edgeBuilder.setIndexHint(IndexHint.DO_NOT_INDEX);
        edgeBuilder.save(authorizations);
    }

    @Override
    public void deleteUserFromWorkspace(final Workspace workspace, final String userId, final User user) {
        if (!hasWritePermissions(workspace.getWorkspaceId(), user)) {
            throw new BcAccessDeniedException(
                    "user " + user.getUserId() + " does not have write access to workspace " + workspace.getWorkspaceId(),
                    user,
                    workspace.getWorkspaceId()
            );
        }

        lockRepository.lock(getLockName(workspace), () -> {
            Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                    user,
                    UserRepository.VISIBILITY_STRING,
                    VISIBILITY_STRING,
                    workspace.getWorkspaceId()
            );
            Vertex userVertex = getGraph().getVertex(userId, authorizations);
            if (userVertex == null) {
                throw new BcResourceNotFoundException("Could not find user: " + userId, userId);
            }
            Vertex workspaceVertex = getVertexFromWorkspace(workspace, true, authorizations);
            List<Edge> edges = stream(workspaceVertex.getEdges(
                    userVertex,
                    Direction.BOTH,
                    WORKSPACE_TO_USER_RELATIONSHIP_NAME,
                    authorizations
            )).collect(Collectors.toList());
            for (Edge edge : edges) {
                getGraph().softDeleteEdge(edge, authorizations);
            }
            getGraph().flush();

            clearCache();
        });
        fireWorkspaceDeleteUser(workspace, userId, user);
    }

    @Override
    public boolean hasCommentPermissions(String workspaceId, User user) {
        if (user instanceof SystemUser) {
            return true;
        }

        String cacheKey = workspaceId + user.getUserId();
        Boolean hasCommentAccess = usersWithCommentAccessCache.getIfPresent(cacheKey);
        if (hasCommentAccess != null && hasCommentAccess) {
            return true;
        }

        List<WorkspaceUser> usersWithAccess = findUsersWithAccess(workspaceId, user);
        for (WorkspaceUser userWithAccess : usersWithAccess) {
            if (userWithAccess.getUserId().equals(user.getUserId())
                    && WorkspaceAccess.hasCommentPermissions(userWithAccess.getWorkspaceAccess())) {
                usersWithCommentAccessCache.put(cacheKey, true);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasWritePermissions(String workspaceId, User user) {
        if (user instanceof SystemUser) {
            return true;
        }

        String cacheKey = workspaceId + user.getUserId();
        Boolean hasWriteAccess = usersWithWriteAccessCache.getIfPresent(cacheKey);
        if (hasWriteAccess != null && hasWriteAccess) {
            return true;
        }

        List<WorkspaceUser> usersWithAccess = findUsersWithAccess(workspaceId, user);
        for (WorkspaceUser userWithAccess : usersWithAccess) {
            if (userWithAccess.getUserId().equals(user.getUserId())
                    && WorkspaceAccess.hasWritePermissions(userWithAccess.getWorkspaceAccess())) {
                usersWithWriteAccessCache.put(cacheKey, true);
                return true;
            }
        }
        return false;
    }

    @Override
    @Traced
    public boolean hasReadPermissions(String workspaceId, User user) {
        if(user == null || workspaceId == null)
            return false;

        if (user instanceof SystemUser) {
            return true;
        }

        String cacheKey = workspaceId + user.getUserId();
        Boolean hasReadAccess = usersWithReadAccessCache.getIfPresent(cacheKey);
        if (hasReadAccess != null && hasReadAccess) {
            return true;
        }

        List<WorkspaceUser> usersWithAccess = findUsersWithAccess(workspaceId, user);
        for (WorkspaceUser userWithAccess : usersWithAccess) {
            if (userWithAccess.getUserId().equals(user.getUserId())
                    && WorkspaceAccess.hasReadPermissions(userWithAccess.getWorkspaceAccess())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public UpdateUserOnWorkspaceResult updateUserOnWorkspace(
            final Workspace workspace,
            final String userId,
            final WorkspaceAccess workspaceAccess,
            final User user
    ) {
        if (!hasWritePermissions(workspace.getWorkspaceId(), user)) {
            throw new BcAccessDeniedException(
                    "user " + user.getUserId() + " does not have write access to workspace " + workspace.getWorkspaceId(),
                    user,
                    workspace.getWorkspaceId()
            );
        }

        return lockRepository.lock(getLockName(workspace), () -> {
            Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(
                    user,
                    VISIBILITY_STRING,
                    workspace.getWorkspaceId()
            );
            Vertex otherUserVertex;
            if (userRepository instanceof GeUserRepository) {
                otherUserVertex = ((GeUserRepository) userRepository).findByIdUserVertex(userId);
            } else {
                otherUserVertex = getGraph().getVertex(userId, authorizations);
            }
            if (otherUserVertex == null) {
                throw new BcResourceNotFoundException("Could not find user: " + userId, userId);
            }

            Vertex workspaceVertex = getVertexFromWorkspace(workspace, true, authorizations);
            if (workspaceVertex == null) {
                throw new BcResourceNotFoundException(
                        "Could not find workspace vertex: " + workspace.getWorkspaceId(),
                        workspace.getWorkspaceId()
                );
            }

            UpdateUserOnWorkspaceResult result;
            List<Edge> existingEdges = stream(workspaceVertex.getEdges(
                    otherUserVertex,
                    Direction.OUT,
                    WORKSPACE_TO_USER_RELATIONSHIP_NAME,
                    authorizations
            )).collect(Collectors.toList());
            if (existingEdges.size() > 0) {
                for (Edge existingEdge : existingEdges) {
                    WorkspaceSchema.WORKSPACE_TO_USER_ACCESS.setProperty(
                            existingEdge,
                            workspaceAccess.toString(),
                            VISIBILITY.getVisibility(),
                            authorizations
                    );
                }
                result = UpdateUserOnWorkspaceResult.UPDATE;
            } else {
                EdgeBuilder edgeBuilder = getGraph().prepareEdge(
                        workspaceVertex,
                        otherUserVertex,
                        WORKSPACE_TO_USER_RELATIONSHIP_NAME,
                        VISIBILITY.getVisibility()
                );
                WorkspaceSchema.WORKSPACE_TO_USER_ACCESS.setProperty(
                        edgeBuilder,
                        workspaceAccess.toString(),
                        VISIBILITY.getVisibility()
                );
                edgeBuilder.save(authorizations);
                result = UpdateUserOnWorkspaceResult.ADD;
            }

            getGraph().flush();

            clearCache();

            fireWorkspaceUpdateUser(workspace, userId, workspaceAccess, user);

            return result;
        });
    }
}
