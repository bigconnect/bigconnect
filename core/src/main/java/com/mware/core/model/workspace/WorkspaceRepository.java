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

import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcAccessDeniedException;
import com.mware.core.model.clientapi.dto.ClientApiWorkspace;
import com.mware.core.model.clientapi.dto.WorkspaceAccess;
import com.mware.core.model.properties.WorkspaceSchema;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.Vertex;
import com.mware.ge.util.ConvertingIterable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.ge.util.IterableUtils.toList;

public abstract class WorkspaceRepository {
    public static final String TO_ENTITY_ID_SEPARATOR = "_TO_ENTITY_";
    public static final String VISIBILITY_STRING = "workspace";
    public static final String VISIBILITY_PRODUCT_STRING = "workspace_product";
    public static final BcVisibility VISIBILITY = new BcVisibility(VISIBILITY_STRING);
    public static final String WORKSPACE_CONCEPT_NAME = WorkspaceSchema.WORKSPACE_CONCEPT_NAME;
    public static final String WORKSPACE_TO_ENTITY_RELATIONSHIP_NAME = WorkspaceSchema.WORKSPACE_TO_ENTITY_RELATIONSHIP_NAME;
    public static final String WORKSPACE_TO_USER_RELATIONSHIP_NAME = WorkspaceSchema.WORKSPACE_TO_USER_RELATIONSHIP_NAME;
    public static final String WORKSPACE_ID_PREFIX = "WORKSPACE_";
    private final Graph graph;
    private final Configuration configuration;
    private final VisibilityTranslator visibilityTranslator;
    private final TermMentionRepository termMentionRepository;
    private final SchemaRepository schemaRepository;
    private final WorkQueueRepository workQueueRepository;
    private final WebQueueRepository webQueueRepository;
    private final AuthorizationRepository authorizationRepository;
    private Collection<WorkspaceListener> workspaceListeners;

    protected WorkspaceRepository(
            Graph graph,
            Configuration configuration,
            VisibilityTranslator visibilityTranslator,
            TermMentionRepository termMentionRepository,
            SchemaRepository schemaRepository,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            AuthorizationRepository authorizationRepository
    ) {
        this.graph = graph;
        this.configuration = configuration;
        this.visibilityTranslator = visibilityTranslator;
        this.termMentionRepository = termMentionRepository;
        this.schemaRepository = schemaRepository;
        this.workQueueRepository = workQueueRepository;
        this.webQueueRepository = webQueueRepository;
        this.authorizationRepository = authorizationRepository;
    }

    public static String getWorkspaceToEntityEdgeId(String workspaceVertexId, String entityVertexId) {
        return workspaceVertexId + TO_ENTITY_ID_SEPARATOR + entityVertexId;
    }

    public abstract void delete(Workspace workspace, User user);

    public Workspace findById(String workspaceId, User user) {
        return findById(workspaceId, false, user);
    }

    public abstract Workspace findById(String workspaceId, boolean includeHidden, User user);

    public Iterable<Workspace> findByIds(final Iterable<String> workspaceIds, final User user) {
        return new ConvertingIterable<String, Workspace>(workspaceIds) {
            @Override
            protected Workspace convert(String workspaceId) {
                if (workspaceId == null) {
                    return null;
                }
                try {
                    return findById(workspaceId, user);
                } catch (BcAccessDeniedException ex) {
                    return null;
                }
            }
        };
    }

    public abstract Vertex getVertex(String workspaceId, User user);

    public String getDefaultWorkspaceName(User user) {
        return String.format("Default – %s", user.getDisplayName());
    }

    public abstract Workspace add(String workspaceId, String title, User user);

    public Workspace add(User user) {
        return add(null, user);
    }

    public Workspace add(String title, User user) {
        if (title == null) {
            title = getDefaultWorkspaceName(user);
        }
        return add(null, title, user);
    }

    public void clearCache() {
    }

    /**
     * Finds all workspaces the given user has access to. Including workspaces shared to that user.
     */
    public abstract Iterable<Workspace> findAllForUser(User user);

    /**
     * Finds all workspaces regardless of access.
     *
     * @param user a user with access to all workspaces such as system user.
     */
    public abstract Iterable<Workspace> findAll(User user);

    public abstract void setTitle(Workspace workspace, String title, User user);

    public abstract void setStaging(Workspace workspace, Boolean staging, User user);

    public abstract List<WorkspaceUser> findUsersWithAccess(String workspaceId, User user);

    public List<WorkspaceEntity> findEntities(Workspace workspace, User user) {
        return findEntities(workspace, false, user);
    }

    public List<WorkspaceEntity> findEntities(Workspace workspace, boolean fetchVertices, User user) {
        return findEntities(workspace, fetchVertices, user, true, false);
    }

    public abstract List<WorkspaceEntity> findEntities(Workspace workspace, boolean fetchVertices, User user, boolean lock, boolean hidden);


    public Workspace copy(Workspace workspace, User user) {
        return copyTo(workspace, user, user);
    }

    public Workspace copyTo(Workspace workspace, User destinationUser, User user) {
        return add("Copy of " + workspace.getDisplayTitle(), destinationUser);
    }

    public abstract void softDeleteEntitiesFromWorkspace(
            Workspace workspace,
            List<String> entityIdsToDelete,
            User authUser
    );

    public abstract void deleteUserFromWorkspace(Workspace workspace, String userId, User user);

    public abstract UpdateUserOnWorkspaceResult updateUserOnWorkspace(
            Workspace workspace,
            String userId,
            WorkspaceAccess workspaceAccess,
            User user
    );

    public enum UpdateUserOnWorkspaceResult {
        ADD, UPDATE
    }

    public String getCreatorUserId(String workspaceId, User user) {
        for (WorkspaceUser workspaceUser : findUsersWithAccess(workspaceId, user)) {
            if (workspaceUser.isCreator()) {
                return workspaceUser.getUserId();
            }
        }
        return null;
    }

    public String getLockName(Workspace workspace) {
        return getLockName(workspace.getWorkspaceId());
    }

    public String getLockName(String workspaceId) {
        return "WORKSPACE_" + workspaceId;
    }


    public abstract boolean hasCommentPermissions(String workspaceId, User user);

    public abstract boolean hasWritePermissions(String workspaceId, User user);

    public abstract boolean hasReadPermissions(String workspaceId, User user);

    public JSONArray toJson(Iterable<Workspace> workspaces, User user) {
        JSONArray resultJson = new JSONArray();
        for (Workspace workspace : workspaces) {
            resultJson.put(toJson(workspace, user));
        }
        return resultJson;
    }

    public JSONObject toJson(Workspace workspace, User user) {
        checkNotNull(workspace, "workspace cannot be null");
        checkNotNull(user, "user cannot be null");

        try {
            JSONObject workspaceJson = new JSONObject();
            workspaceJson.put("workspaceId", workspace.getWorkspaceId());
            workspaceJson.put("title", workspace.getDisplayTitle());

            String creatorUserId = getCreatorUserId(workspace.getWorkspaceId(), user);
            if (creatorUserId != null) {
                workspaceJson.put("createdBy", creatorUserId);
                workspaceJson.put("sharedToUser", !creatorUserId.equals(user.getUserId()));
            }
            workspaceJson.put("editable", hasWritePermissions(workspace.getWorkspaceId(), user));

            JSONArray usersJson = new JSONArray();
            for (WorkspaceUser workspaceUser : findUsersWithAccess(workspace.getWorkspaceId(), user)) {
                String userId = workspaceUser.getUserId();
                JSONObject userJson = new JSONObject();
                userJson.put("userId", userId);
                userJson.put("access", workspaceUser.getWorkspaceAccess().toString().toLowerCase());
                usersJson.put(userJson);
            }
            workspaceJson.put("users", usersJson);

            return workspaceJson;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public ClientApiWorkspace toClientApi(
            Workspace workspace,
            User user,
            Authorizations authorizations
    ) {
        checkNotNull(workspace, "workspace cannot be null");
        checkNotNull(user, "user cannot be null");

        try {
            ClientApiWorkspace workspaceClientApi = new ClientApiWorkspace();
            workspaceClientApi.setWorkspaceId(workspace.getWorkspaceId());
            workspaceClientApi.setTitle(workspace.getDisplayTitle());
            workspaceClientApi.setStaging(workspace.getStaging());

            String creatorUserId = getCreatorUserId(workspace.getWorkspaceId(), user);
            if (creatorUserId == null) {
                workspaceClientApi.setSharedToUser(true);
            } else {
                workspaceClientApi.setCreatedBy(creatorUserId);
                workspaceClientApi.setSharedToUser(!creatorUserId.equals(user.getUserId()));
            }
            workspaceClientApi.setEditable(hasWritePermissions(workspace.getWorkspaceId(), user));
            workspaceClientApi.setCommentable(hasCommentPermissions(workspace.getWorkspaceId(), user));

            for (WorkspaceUser u : findUsersWithAccess(workspace.getWorkspaceId(), user)) {
                String userId = u.getUserId();
                ClientApiWorkspace.User workspaceUser = new ClientApiWorkspace.User();
                workspaceUser.setUserId(userId);
                workspaceUser.setAccess(u.getWorkspaceAccess());
                workspaceClientApi.addUser(workspaceUser);
            }

            return workspaceClientApi;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    protected Graph getGraph() {
        return graph;
    }

    public abstract void updateEntitiesOnWorkspace(Workspace workspace, Collection<String> vertexIds, User user);

    public void updateEntityOnWorkspace(
            Workspace workspace,
            String vertexId,
            User user
    ) {
        Collection<String> vertexIds = new ArrayList<>();
        vertexIds.add(vertexId);
        updateEntitiesOnWorkspace(workspace, vertexIds, user);
    }

    public void updateEntityOnWorkspace(
            String workspaceId,
            String vertexId,
            User user
    ) {
        Workspace workspace = findById(workspaceId, user);
        updateEntityOnWorkspace(workspace, vertexId, user);
    }

    @Deprecated
    public List<String> findEntityVertexIds(Workspace workspace, User user) {
        List<WorkspaceEntity> workspaceEntities = findEntities(workspace, user);
        return toList(new ConvertingIterable<WorkspaceEntity, String>(workspaceEntities) {
            @Override
            protected String convert(WorkspaceEntity workspaceEntity) {
                return workspaceEntity.getEntityVertexId();
            }
        });
    }

    protected VisibilityTranslator getVisibilityTranslator() {
        return visibilityTranslator;
    }

    protected TermMentionRepository getTermMentionRepository() {
        return termMentionRepository;
    }

    protected SchemaRepository getSchemaRepository() {
        return schemaRepository;
    }

    protected WorkQueueRepository getWorkQueueRepository() {
        return workQueueRepository;
    }

    public WebQueueRepository getWebQueueRepository() {
        return webQueueRepository;
    }

    protected void fireWorkspaceBeforeDelete(Workspace workspace, User user) {
        for (WorkspaceListener workspaceListener : getWorkspaceListeners()) {
            workspaceListener.workspaceBeforeDelete(workspace, user);
        }
    }

    protected void fireWorkspaceUpdateEntities(Workspace workspace, Collection<String> vertexIds, User user) {
        for (WorkspaceListener workspaceListener : getWorkspaceListeners()) {
            workspaceListener.workspaceUpdateEntities(workspace, vertexIds, user);
        }
    }

    protected void fireWorkspaceUpdateUser(Workspace workspace, String userId, WorkspaceAccess workspaceAccess, User user) {
        for (WorkspaceListener workspaceListener : getWorkspaceListeners()) {
            workspaceListener.workspaceUpdateUser(workspace, userId, workspaceAccess, user);
        }
    }

    protected void fireWorkspaceAdded(Workspace workspace, User user) {
        for (WorkspaceListener workspaceListener : getWorkspaceListeners()) {
            workspaceListener.workspaceAdded(workspace, user);
        }
    }

    protected void fireWorkspaceDeleteUser(Workspace workspace, String userId, User user) {
        for (WorkspaceListener workspaceListener : getWorkspaceListeners()) {
            workspaceListener.workspaceDeleteUser(workspace, userId, user);
        }
    }

    public Collection<WorkspaceListener> getWorkspaceListeners() {
        if (workspaceListeners == null) {
            workspaceListeners = InjectHelper.getInjectedServices(WorkspaceListener.class, configuration);
        }
        return workspaceListeners;
    }

    protected AuthorizationRepository getAuthorizationRepository() {
        return authorizationRepository;
    }

    public boolean isStagingEnabled(String workspaceId, User user) {
        Workspace workspace = findById(workspaceId, user);
        return isStagingEnabled(workspace);
    }

    public boolean isStagingEnabled(Workspace workspace) {
        return workspace == null || workspace.getStaging() == null ? isGlobalStagingEnabled() : workspace.getStaging();
    }

    public boolean isGlobalStagingEnabled() {
        return !configuration.getBoolean(Configuration.WORKSPACE_AUTO_PUBLISH, false);
    }
}

