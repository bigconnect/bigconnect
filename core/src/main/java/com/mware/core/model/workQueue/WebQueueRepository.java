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
package com.mware.core.model.workQueue;

import com.google.common.collect.Sets;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.exception.BcAccessDeniedException;
import com.mware.core.exception.BcException;
import com.mware.core.lifecycle.LifecycleAdapter;
import com.mware.core.model.clientapi.dto.ClientApiWorkspace;
import com.mware.core.model.clientapi.dto.UserStatus;
import com.mware.core.model.notification.SystemNotification;
import com.mware.core.model.notification.UserNotification;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.types.BcPropertyUpdate;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workspace.Workspace;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.model.workspace.WorkspaceUser;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class WebQueueRepository extends LifecycleAdapter  {
    private UserRepository userRepository;
    private AuthorizationRepository authorizationRepository;
    private WorkspaceRepository workspaceRepository;

    protected WebQueueRepository() {
    }

    public void broadcastLongRunningProcessDeleted(JSONObject longRunningProcessQueueItem) {
        String userId = longRunningProcessQueueItem.optString("userId");
        checkNotNull(userId, "userId cannot be null");
        JSONObject json = new JSONObject();
        json.put("type", "longRunningProcessDeleted");
        JSONObject permissions = new JSONObject();
        JSONArray users = new JSONArray();
        users.put(userId);
        permissions.put("users", users);
        json.put("permissions", permissions);
        JSONObject data = new JSONObject();
        data.put("processId", longRunningProcessQueueItem.get("id"));
        json.put("data", data);
        broadcastJson(json);
    }

    public void broadcastLongRunningProcessChange(JSONObject longRunningProcessQueueItem) {
        String userId = longRunningProcessQueueItem.optString("userId");
        checkNotNull(userId, "userId cannot be null");
        JSONObject json = new JSONObject();
        json.put("type", "longRunningProcessChange");
        JSONObject permissions = new JSONObject();
        JSONArray users = new JSONArray();
        users.put(userId);
        permissions.put("users", users);
        json.put("permissions", permissions);
        JSONObject dataJson = new JSONObject(longRunningProcessQueueItem.toString());

        /// because results can get quite large we don't want this going on in a web socket message
        if (dataJson.has("results")) {
            dataJson.remove("results");
        }

        json.put("data", dataJson);
        broadcastJson(json);
    }

    public void broadcastWorkProductAncillaryChange(String workProductId, String workspaceId, String ancillaryId, User user, String skipSourceGuid) {
        JSONObject json = new JSONObject();
        json.put("type", "workProductAncillaryChange");
        json.put("permissions", getPermissionsWithWorkspace(workspaceId));
        JSONObject dataJson = new JSONObject();
        dataJson.put("id", ancillaryId);
        dataJson.put("workspaceId", workspaceId);
        dataJson.put("productId", workProductId);
        dataJson.putOpt("sourceGuid", skipSourceGuid);
        json.put("data", dataJson);
        broadcastJson(json);
    }

    public void broadcastWorkProductPreviewChange(String workProductId, String workspaceId, User user, String md5) {
        JSONObject json = new JSONObject();
        json.put("type", "workProductPreviewChange");

        JSONObject permissions = new JSONObject();
        JSONArray users = new JSONArray();
        users.put(user.getUserId());
        permissions.put("users", users);
        json.put("permissions", permissions);

        JSONObject dataJson = new JSONObject();
        dataJson.put("id", workProductId);
        dataJson.put("workspaceId", workspaceId);
        dataJson.putOpt("md5", md5);
        json.put("data", dataJson);
        broadcastJson(json);
    }

    public void broadcastWorkProductDelete(String workProductId, ClientApiWorkspace workspace) {
        JSONObject json = new JSONObject();
        json.put("type", "workProductDelete");
        json.put("permissions", getPermissionsWithUsers(workspace, null));
        JSONObject dataJson = new JSONObject();
        dataJson.put("id", workProductId);
        json.put("data", dataJson);
        broadcastJson(json);
    }

    public void broadcastEdgeDeletion(Edge edge) {
        JSONObject dataJson = new JSONObject();
        if (edge != null) {
            dataJson.put("edgeId", edge.getId());
            dataJson.put("outVertexId", edge.getVertexId(Direction.OUT));
            dataJson.put("inVertexId", edge.getVertexId(Direction.IN));
        }

        JSONObject json = new JSONObject();
        json.put("type", "edgeDeletion");
        json.put("data", dataJson);
        broadcastJson(json);
    }

    public void broadcastVerticesDeletion(String vertexId) {
        JSONArray verticesDeleted = new JSONArray();
        verticesDeleted.put(vertexId);
        broadcastVerticesDeletion(verticesDeleted);
    }

    public void broadcastVerticesDeletion(JSONArray verticesDeleted) {
        JSONObject dataJson = new JSONObject();
        if (verticesDeleted != null) {
            dataJson.put("vertexIds", verticesDeleted);
        }

        JSONObject json = new JSONObject();
        json.put("type", "verticesDeleted");
        json.put("data", dataJson);
        broadcastJson(json);
    }

    public void pushOntologyConceptsChange(String workspaceId, List<String> ids) {
        pushOntologyChange(workspaceId, SchemaAction.Update, ids, null, null);
    }

    public void pushOntologyConceptsChange(String workspaceId, String... ids) {
        pushOntologyChange(workspaceId, SchemaAction.Update, Arrays.asList(ids), null, null);
    }

    public void pushOntologyPropertiesChange(String workspaceId, String... ids) {
        pushOntologyChange(workspaceId, SchemaAction.Update, null, null, Arrays.asList(ids));
    }

    public void pushOntologyPropertiesChange(String workspaceId, List<String> ids) {
        pushOntologyChange(workspaceId, SchemaAction.Update, null, null, ids);
    }

    public void pushOntologyRelationshipsChange(String workspaceId, List<String> ids) {
        pushOntologyChange(workspaceId, SchemaAction.Update, null, ids, null);
    }

    public void pushOntologyRelationshipsChange(String workspaceId, String... ids) {
        pushOntologyChange(workspaceId, SchemaAction.Update, null, Arrays.asList(ids), null);
    }

    public void pushOntologyChange(String workspaceId, SchemaAction action, Iterable<String> conceptIds, Iterable<String> relationshipIds, Iterable<String> propertyIds) {
        JSONObject json = new JSONObject();
        json.put("type", "ontologyChange");

        JSONObject data = new JSONObject();
        data.put("action", action.toString());
        data.put("idType", action.equals(SchemaAction.Update) ? "id" : "iri");

        if (workspaceId != null) {
            data.put("workspaceId", workspaceId);
            json.put("permissions", getPermissionsWithWorkspace(workspaceId));
        }
        if (conceptIds != null || relationshipIds != null || propertyIds != null) {
            data.put("conceptIds", conceptIds == null ? new JSONArray() : new JSONArray(Sets.newHashSet(conceptIds)));
            data.put("propertyIds", propertyIds == null ? new JSONArray() : new JSONArray(Sets.newHashSet(propertyIds)));
            data.put("relationshipIds", relationshipIds == null ? new JSONArray() : new JSONArray(Sets.newHashSet(relationshipIds)));
        }
        json.put("data", data);
        broadcastJson(json);
    }

    public void pushTextUpdated(String vertexId) {
        pushTextUpdated(vertexId, Priority.NORMAL);
    }

    public void pushTextUpdated(String vertexId, Priority priority) {
        if (shouldBroadcast(priority)) {
            broadcastTextUpdated(vertexId);
        }
    }

    protected void broadcastTextUpdated(String vertexId) {
        JSONObject dataJson = new JSONObject();
        if (vertexId != null) {
            dataJson.put("graphVertexId", vertexId);
        }

        JSONObject json = new JSONObject();
        json.put("type", "textUpdated");
        json.put("data", dataJson);
        broadcastJson(json);
    }

    public void pushUserAccessChange(User user) {
        JSONObject json = new JSONObject();
        json.put("type", "userAccessChange");
        json.put("permissions", getPermissionsWithUserIds(user.getUserId()));
        json.put("data", getUserRepository().toJsonWithAuths(user));
        broadcastJson(json);
    }

    public void broadcastUserStatusChange(User user, UserStatus status) {
        JSONObject json = new JSONObject();
        json.put("type", "userStatusChange");
        JSONObject data = UserRepository.toJson(user);
        data.put("status", status.toString());
        json.put("data", data);
        broadcastJson(json);
    }

    public void broadcastWorkProductChange(String workProductId, String skipSourceGuid, String workspaceId, JSONObject permissions) {
        JSONObject json = new JSONObject();
        json.put("type", "workProductChange");
        json.put("permissions", permissions);
        JSONObject dataJson = new JSONObject();
        dataJson.put("id", workProductId);
        dataJson.put("workspaceId", workspaceId);
        dataJson.putOpt("sourceGuid", skipSourceGuid);
        json.put("data", dataJson);
        broadcastJson(json);
    }

    public void broadcastUserWorkspaceChange(User user, String workspaceId) {
        JSONObject json = new JSONObject();
        json.put("type", "userWorkspaceChange");
        JSONObject data = UserRepository.toJson(user);
        data.put("workspaceId", workspaceId);
        json.put("data", data);
        broadcastJson(json);
    }

    public void broadcastWorkspace(
            ClientApiWorkspace workspace,
            List<ClientApiWorkspace.User> previousUsers,
            String changedByUserId,
            String changedBySourceGuid
    ) {
        User changedByUser = getUserRepository().findById(changedByUserId);
        Workspace ws = getWorkspaceRepository().findById(workspace.getWorkspaceId(), changedByUser);

        previousUsers.forEach(workspaceUser -> {
            boolean isChangingUser = workspaceUser.getUserId().equals(changedByUserId);

            User user = isChangingUser ? changedByUser : getUserRepository().findById(workspaceUser.getUserId());
            Authorizations authorizations = getAuthorizationRepository().getGraphAuthorizations(user, workspace.getWorkspaceId());

            // No need to regenerate client api if changing user
            try {
                ClientApiWorkspace userWorkspace = isChangingUser ? workspace : getWorkspaceRepository().toClientApi(ws, user, authorizations);
                JSONObject json = new JSONObject();
                json.put("type", "workspaceChange");
                json.put("modifiedBy", changedByUserId);
                json.put("permissions", getPermissionsWithUsers(null, Arrays.asList(workspaceUser)));
                json.put("data", new JSONObject(ClientApiConverter.clientApiToString(userWorkspace)));
                json.putOpt("sourceGuid", changedBySourceGuid);
                broadcastJson(json);
            } catch (BcAccessDeniedException e) {
                /* Ignore push message if lost access */
            }
        });
    }

    public void pushWorkspaceDelete(ClientApiWorkspace workspace) {
        JSONObject json = new JSONObject();
        json.put("type", "workspaceDelete");
        json.put("permissions", getPermissionsWithUsers(workspace, null));
        json.put("workspaceId", workspace.getWorkspaceId());
        broadcastJson(json);
    }

    public void pushWorkspaceDelete(String workspaceId, String userId) {
        JSONObject json = new JSONObject();
        json.put("type", "workspaceDelete");
        JSONObject permissions = new JSONObject();
        JSONArray users = new JSONArray();
        users.put(userId);
        permissions.put("users", users);
        json.put("permissions", permissions);
        json.put("workspaceId", workspaceId);
        broadcastJson(json);
    }

    public void pushSessionExpiration(String userId, String sessionId) {
        JSONObject json = new JSONObject();
        json.put("type", "sessionExpiration");

        JSONObject permissions = new JSONObject();
        JSONArray users = new JSONArray();
        users.put(userId);
        permissions.put("users", users);
        JSONArray sessionIds = new JSONArray();
        sessionIds.put(sessionId);
        permissions.put("sessionIds", sessionIds);
        json.put("permissions", permissions);
        json.putOpt("sessionId", sessionId);
        broadcastJson(json);
    }

    public void pushUserNotification(UserNotification notification) {
        JSONObject json = new JSONObject();
        json.put("type", "notification");

        JSONObject permissions = new JSONObject();
        JSONArray users = new JSONArray();
        users.put(notification.getUserId());
        permissions.put("users", users);
        json.put("permissions", permissions);

        JSONObject data = new JSONObject();
        json.put("data", data);
        data.put("notification", notification.toJSONObject());
        broadcastJson(json);
    }

    public void pushSystemNotification(SystemNotification notification) {
        JSONObject json = new JSONObject();
        json.put("type", "notification");
        JSONObject data = new JSONObject();
        json.put("data", data);
        data.put("notification", notification.toJSONObject());
        broadcastJson(json);
    }

    public void pushSystemNotificationUpdate(SystemNotification notification) {
        JSONObject json = new JSONObject();
        json.put("type", "systemNotificationUpdated");
        JSONObject data = new JSONObject();
        json.put("data", data);
        data.put("notification", notification.toJSONObject());
        broadcastJson(json);
    }

    public void pushSystemNotificationEnded(String notificationId) {
        JSONObject json = new JSONObject();
        json.put("type", "systemNotificationEnded");
        JSONObject data = new JSONObject();
        json.put("data", data);
        data.put("notificationId", notificationId);
        broadcastJson(json);
    }

    public void broadcastElementImage(Element element) {
        try {
            // TODO: only broadcast to workspace users if sandboxStatus is PRIVATE
            JSONObject json = new JSONObject();
            json.put("type", "entityImageUpdated");

            JSONObject dataJson = new JSONObject();
            dataJson.put("graphVertexId", element.getId());

            json.put("data", dataJson);

            broadcastJson(json);
        } catch (Exception ex) {
            throw new BcException("Could not broadcast property change", ex);
        }
    }

    public void broadcastPropertiesChange(Element element, Iterable<BcPropertyUpdate> properties, String workspaceId, Priority priority) {
        for (BcPropertyUpdate propertyUpdate : properties) {
            String propertyName = propertyUpdate.getPropertyName();

            if (shouldBroadcastGraphPropertyChange(propertyName, priority)) {
                broadcastPropertyChange(element, propertyUpdate.getPropertyKey(), propertyName, workspaceId);
            }
        }
    }

    public void broadcastPropertyChange(
            Element element,
            String propertyKey,
            String propertyName,
            String workspaceId
    ) {
        try {
            JSONObject json;
            if (element instanceof Vertex) {
                json = new JSONObject();
                json.put("type", "propertyChange");

                JSONObject dataJson = new JSONObject();
                dataJson.put("graphVertexId", element.getId());
                dataJson.putOpt("workspaceId", workspaceId);

                json.put("data", dataJson);
            } else if (element instanceof Edge) {
                Edge edge = (Edge) element;

                // TODO: only broadcast to workspace users if sandboxStatus is PRIVATE
                json = new JSONObject();
                json.put("type", "propertyChange");

                JSONObject dataJson = new JSONObject();
                dataJson.put("graphEdgeId", edge.getId());
                dataJson.put("outVertexId", edge.getVertexId(Direction.OUT));
                dataJson.put("inVertexId", edge.getVertexId(Direction.IN));
                dataJson.putOpt("workspaceId", workspaceId);

                json.put("data", dataJson);
            } else {
                throw new BcException("Unexpected element type: " + element.getClass().getName());
            }

            broadcastJson(json);
        } catch (Exception ex) {
            throw new BcException("Could not broadcast property change", ex);
        }
    }

    public boolean shouldBroadcast(Priority priority) {
        return priority != Priority.LOW;
    }

    public boolean shouldBroadcastGraphPropertyChange(String propertyName, Priority priority) {
        return shouldBroadcast(priority) &&
                !MediaBcSchema.VIDEO_FRAME.getPropertyName().equals(propertyName);
    }

    public abstract void broadcastJson(JSONObject json);

    private JSONObject getPermissionsWithUserIds(String ...userIds) {
        JSONObject permissions = new JSONObject();
        JSONArray users = new JSONArray();
        for (String userId : userIds) {
            users.put(userId);
        }
        permissions.put("users", users);
        return permissions;
    }

    private JSONObject getPermissionsWithWorkspace(String workspaceId) {
        List<WorkspaceUser> users = getWorkspaceRepository().findUsersWithAccess(workspaceId, new SystemUser());
        return getPermissionsWithUsers(users);
    }

    private JSONObject getPermissionsWithUsers(List<WorkspaceUser> workspaceUsers) {
        JSONObject permissions = new JSONObject();
        JSONArray users = new JSONArray();
        if (workspaceUsers != null) {
            for (WorkspaceUser workspaceUser : workspaceUsers) {
                users.put(workspaceUser.getUserId());
            }
        }
        permissions.put("users", users);
        return permissions;
    }

    public JSONObject getPermissionsWithUsers(
            ClientApiWorkspace workspace,
            List<ClientApiWorkspace.User> previousUsers
    ) {
        JSONObject permissions = new JSONObject();
        JSONArray users = new JSONArray();
        if (previousUsers != null) {
            for (ClientApiWorkspace.User user : previousUsers) {
                users.put(user.getUserId());
            }
        }
        if (workspace != null) {
            for (ClientApiWorkspace.User user : workspace.getUsers()) {
                users.put(user.getUserId());
            }
        }
        permissions.put("users", users);
        return permissions;
    }

    public void broadcastPublishVertexDelete(Vertex vertex) {
        broadcastPublish(vertex, null, null, PublishType.DELETE);
    }

    public void broadcastPublishVertex(Vertex vertex) {
        broadcastPublish(vertex, null, null, PublishType.TO_PUBLIC);
    }

    public void broadcastUndoVertexDelete(Vertex vertex) {
        broadcastPublish(vertex, null, null, PublishType.UNDO_DELETE);
    }

    public void broadcastUndoVertex(Vertex vertex) {
        broadcastPublish(vertex, null, null, PublishType.UNDO);
    }

    public void broadcastPublishPropertyDelete(Element element, String key, String name) {
        broadcastPublish(element, key, name, PublishType.DELETE);
    }

    public void broadcastPublishProperty(Element element, String key, String name) {
        broadcastPublish(element, key, name, PublishType.TO_PUBLIC);
    }

    public void broadcastUndoPropertyDelete(Element element, String key, String name) {
        broadcastPublish(element, key, name, PublishType.UNDO_DELETE);
    }

    public void broadcastUndoProperty(Element element, String key, String name) {
        broadcastPublish(element, key, name, PublishType.UNDO);
    }

    public void broadcastPublishEdgeDelete(Edge edge) {
        broadcastPublish(edge, null, null, PublishType.DELETE);
    }

    public void broadcastPublishEdge(Edge edge) {
        broadcastPublish(edge, null, null, PublishType.TO_PUBLIC);
    }

    public void broadcastUndoEdgeDelete(Edge edge) {
        broadcastPublish(edge, null, null, PublishType.UNDO_DELETE);
    }

    public void broadcastUndoEdge(Edge edge) {
        broadcastPublish(edge, null, null, PublishType.UNDO);
    }

    public void broadcastPublish(Element element, String propertyKey, String propertyName, PublishType publishType) {
        try {
            JSONObject json;
            if (element instanceof Vertex) {
                json = new JSONObject();
                json.put("type", "publish");

                JSONObject dataJson = new JSONObject();
                dataJson.put("graphVertexId", element.getId());
                dataJson.put("publishType", publishType.getJsonString());
                if (propertyName == null) {
                    dataJson.put("objectType", "vertex");
                } else {
                    dataJson.put("objectType", "property");
                }
                json.put("data", dataJson);

            } else if (element instanceof Edge) {
                json = new JSONObject();
                json.put("type", "publish");

                JSONObject dataJson = new JSONObject();
                dataJson.put("graphEdgeId", element.getId());
                dataJson.put("publishType", publishType.getJsonString());
                if (propertyName == null) {
                    dataJson.put("objectType", "edge");
                } else {
                    dataJson.put("objectType", "property");
                }
                json.put("data", dataJson);

            } else {
                throw new BcException("Unexpected element type: " + element.getClass().getName());
            }
            broadcastJson(json);
        } catch (Exception ex) {
            throw new BcException("Could not broadcast publish", ex);
        }
    }

    public abstract void subscribeToBroadcastMessages(BroadcastConsumer broadcastConsumer);

    public abstract void unsubscribeFromBroadcastMessages(BroadcastConsumer broadcastConsumer);

    public static abstract class BroadcastConsumer {
        private String consumerKey;

        public abstract void broadcastReceived(JSONObject json);

        public String getConsumerKey() {
            return consumerKey;
        }

        public void setConsumerKey(String consumerKey) {
            this.consumerKey = consumerKey;
        }
    }

    public enum PublishType {
        TO_PUBLIC("toPublic"),
        DELETE("delete"),
        UNDO_DELETE("undoDelete"),
        UNDO("undo");

        private final String jsonString;

        PublishType(String jsonString) {
            this.jsonString = jsonString;
        }

        public String getJsonString() {
            return jsonString;
        }
    }

    public enum SchemaAction {
        Update,
        Delete
    }

    protected UserRepository getUserRepository() {
        if (userRepository == null) {
            userRepository = InjectHelper.getInstance(UserRepository.class);
        }
        return userRepository;
    }

    public void setUserRepository(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    protected WorkspaceRepository getWorkspaceRepository() {
        if (workspaceRepository == null) {
            workspaceRepository = InjectHelper.getInstance(WorkspaceRepository.class);
        }
        return workspaceRepository;
    }

    public void setWorkspaceRepository(WorkspaceRepository workspaceRepository) {
        this.workspaceRepository = workspaceRepository;
    }

    protected AuthorizationRepository getAuthorizationRepository() {
        if (authorizationRepository == null) {
            authorizationRepository = InjectHelper.getInstance(AuthorizationRepository.class);
        }
        return authorizationRepository;
    }

    public void setAuthorizationRepository(AuthorizationRepository authorizationRepository) {
        this.authorizationRepository = authorizationRepository;
    }
}
