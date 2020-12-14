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
package com.mware.core.model.role;

import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.model.clientapi.dto.ClientApiRole;
import com.mware.core.model.clientapi.dto.ClientApiRoles;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.notification.ExpirationAge;
import com.mware.core.model.notification.UserNotification;
import com.mware.core.model.notification.UserNotificationRepository;
import com.mware.core.model.user.UserListener;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import org.json.JSONObject;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AuthorizationRepositoryBase implements AuthorizationRepository {
    private final Graph graph;
    private UserRepository userRepository;
    private final UserNotificationRepository userNotificationRepository;
    private final WebQueueRepository webQueueRepository;
    private final Configuration configuration;
    private Collection<RoleListener> roleListeners;
    private Collection<UserListener> userListeners;
    private Role administratorRole;

    protected AuthorizationRepositoryBase(
            Graph graph,
            UserNotificationRepository userNotificationRepository,
            WebQueueRepository webQueueRepository,
            Configuration configuration
    ) {
        this.graph = graph;
        this.userNotificationRepository = userNotificationRepository;
        this.webQueueRepository = webQueueRepository;
        this.configuration = configuration;
    }

    public com.mware.ge.Authorizations getGraphAuthorizations(String userName, String... additionalAuthorizations) {
        User user = getUserRepository().findByUsername(userName);
        if (user == null)
            return new Authorizations();

        return getGraphAuthorizations(user, additionalAuthorizations);
    }

    @Override
    public com.mware.ge.Authorizations getGraphAuthorizations(User user, String... additionalAuthorizations) {
        checkNotNull(user, "User cannot be null");
        Set<String> userAuthorizations = getRoles(user)
                .stream()
                .map(role -> role.getRoleName())
                .collect(Collectors.toSet());

        Collections.addAll(userAuthorizations, additionalAuthorizations);
        return graph.createAuthorizations(userAuthorizations);
    }

    // Need to late bind since UserRepository injects AuthorizationRepository in constructor
    protected UserRepository getUserRepository() {
        if (userRepository == null) {
            userRepository = InjectHelper.getInstance(UserRepository.class);
        }
        return userRepository;
    }

    protected void sendNotificationToUserAboutAddRole(User user, Role role, User authUser) {
        String title = "Role Added";
        String message = "Role Added: " + role.getRoleName();
        String actionEvent = null;
        JSONObject actionPayload = null;
        ExpirationAge expirationAge = null;
        UserNotification userNotification = userNotificationRepository.createNotification(
                user.getUserId(),
                title,
                message,
                actionEvent,
                actionPayload,
                expirationAge,
                authUser
        );
        webQueueRepository.pushUserNotification(userNotification);
    }

    protected void sendNotificationToUserAboutRemoveRole(User user, Role role, User authUser) {
        String title = "Role Removed";
        String message = "Role Removed: " + role.getRoleName();
        String actionEvent = null;
        JSONObject actionPayload = null;
        ExpirationAge expirationAge = null;
        UserNotification userNotification = userNotificationRepository.createNotification(
                user.getUserId(),
                title,
                message,
                actionEvent,
                actionPayload,
                expirationAge,
                authUser
        );
        webQueueRepository.pushUserNotification(userNotification);
    }

    protected void fireUserAddRoleEvent(User user, Role role) {
        for (UserListener userListener : getUserListeners()) {
            userListener.userAddRole(user, role);
        }
    }

    protected void fireUserRemoveRoleEvent(User user, Role role) {
        for (UserListener userListener : getUserListeners()) {
            userListener.userRemoveRole(user, role);
        }
    }

    protected void fireNewRoleAddedEvent(Role role) {
        for (RoleListener roleListener : getRoleListeners()) {
            roleListener.newRoleAdded(role);
        }
    }

    protected void fireRoleDeletedEvent(Role role) {
        for (RoleListener roleListener : getRoleListeners()) {
            roleListener.roleDeleted(role);
        }
    }

    protected Collection<RoleListener> getRoleListeners() {
        if (roleListeners == null) {
            roleListeners = InjectHelper.getInjectedServices(RoleListener.class, configuration);
        }
        return roleListeners;
    }

    protected Collection<UserListener> getUserListeners() {
        if (userListeners == null) {
            userListeners = InjectHelper.getInjectedServices(UserListener.class, configuration);
        }
        return userListeners;
    }

    public Graph getGraph() {
        return graph;
    }

    @Override
    public ClientApiRoles toClientApi(Iterable<Role> roles) {
        ClientApiRoles clientApiRoles = new ClientApiRoles();
        for (Role role : roles) {
            clientApiRoles.getRoles().add(toClientApi(role));
        }
        return clientApiRoles;
    }

    @Override
    public ClientApiRole toClientApi(Role role) {
        ClientApiRole clientApiRole = new ClientApiRole();
        clientApiRole.setId(role.getRoleId());
        clientApiRole.setRoleName(role.getRoleName());
        clientApiRole.setDescription(role.getDescription());
        clientApiRole.setGlobal(role.isGlobal());

        Set<String> privileges = role.getPrivileges().stream()
                .map(p -> p.getName())
                .collect(Collectors.toSet());

        clientApiRole.getPrivileges().addAll(privileges);

        return clientApiRole;
    }

    public Role getAdministratorRole() {
        if(administratorRole == null) {
            synchronized (this) {
                if(administratorRole == null) {
                    administratorRole = findByName(ADMIN_ROLE);
                    if(administratorRole == null) {
                        administratorRole = addRole(ADMIN_ROLE, "System Administrator", true, Privilege.ALL_BUILT_IN);
                    }
                }
            }
        }
        return administratorRole;
    }
}
