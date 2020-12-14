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
package com.mware.core.model.user;

import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.role.Role;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.orm.SimpleOrmContext;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.ge.values.storable.Value;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.core.util.ClientApiConverter;
import com.mware.core.util.JSONUtil;
import com.mware.core.model.clientapi.dto.ClientApiUser;
import com.mware.core.model.clientapi.dto.ClientApiUsers;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.clientapi.dto.UserStatus;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.mware.ge.util.IterableUtils.toList;

public abstract class UserRepository {
    public static final String GRAPH_USER_ID_PREFIX = "USER_";
    public static final String VISIBILITY_STRING = "user";
    public static final BcVisibility VISIBILITY = new BcVisibility(VISIBILITY_STRING);
    public static final String USER_CONCEPT_NAME = "__usr";
    private final SimpleOrmSession simpleOrmSession;
    private final UserSessionCounterRepository userSessionCounterRepository;
    private final WorkQueueRepository workQueueRepository;
    private final WebQueueRepository webQueueRepository;
    private final LockRepository lockRepository;
    private final Configuration configuration;
    private final AuthorizationRepository authorizationRepository;
    private final PrivilegeRepository privilegeRepository;
    private LongRunningProcessRepository longRunningProcessRepository; // can't inject this because of circular dependencies
    private Collection<UserListener> userListeners;

    protected UserRepository(
            Configuration configuration,
            SimpleOrmSession simpleOrmSession,
            UserSessionCounterRepository userSessionCounterRepository,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            LockRepository lockRepository,
            AuthorizationRepository authorizationRepository,
            PrivilegeRepository privilegeRepository
    ) {
        this.configuration = configuration;
        this.simpleOrmSession = simpleOrmSession;
        this.userSessionCounterRepository = userSessionCounterRepository;
        this.workQueueRepository = workQueueRepository;
        this.webQueueRepository = webQueueRepository;
        this.lockRepository = lockRepository;
        this.authorizationRepository = authorizationRepository;
        this.privilegeRepository = privilegeRepository;
    }

    public abstract User findByUsername(String username);

    public abstract Iterable<User> find(int skip, int limit);

    /*
    simple and likely slow implementation expected to be overridden in production implementations
     */
    public Iterable<User> findByStatus(int skip, int limit, UserStatus status) {
        List<User> allUsers = toList(find(skip, limit));
        List<User> matchingUsers = new ArrayList<>();
        for (User user : allUsers) {
            if (user.getUserStatus() == status) {
                matchingUsers.add(user);
            }
        }
        return matchingUsers;
    }

    public abstract User findById(String userId);

    public abstract User addUser(
            String username,
            String displayName,
            String emailAddress,
            String password
    );

    public void setPassword(User user, String password) {
        byte[] salt = UserPasswordUtil.getSalt();
        byte[] passwordHash = UserPasswordUtil.hashPassword(password, salt);
        setPassword(user, salt, passwordHash);
    }

    public abstract void setPassword(User user, byte[] salt, byte[] passwordHash);

    public abstract boolean isPasswordValid(User user, String password);

    /**
     * Called by web authentication handlers when a user is authenticated
     */
    public abstract void updateUser(User user, AuthorizationContext authorizationContext);

    public abstract User setCurrentWorkspace(String userId, String workspaceId);

    public abstract String getCurrentWorkspaceId(String userId);

    public abstract User setStatus(String userId, UserStatus status);

    public abstract void setDisplayName(User user, String displayName);

    public abstract void setEmailAddress(User user, String emailAddress);

    public abstract void setUiPreferences(User user, JSONObject preferences);

    public JSONObject toJsonWithAuths(User user) {
        JSONObject json = toJson(user);

        JSONArray authorizations = new JSONArray();
        for (Role a : authorizationRepository.getRoles(user)) {
            authorizations.put(a.getRoleName());
        }
        json.put("authorizations", authorizations);

        json.put("uiPreferences", user.getUiPreferences());

        Set<String> privileges = privilegeRepository.getPrivileges(user);
        json.put("privileges", Privilege.toJson(privileges));

        return json;
    }

    /**
     * This is different from the non-private method in that it returns authorizations,
     * long running processes, etc for that user.
     */
    public ClientApiUser toClientApiPrivate(User user) {
        ClientApiUser u = toClientApi(user);

        for (Role a : authorizationRepository.getRoles(user)) {
            u.addAuthorization(a.getRoleName());
        }

        for (JSONObject json : getLongRunningProcesses(user)) {
            u.getLongRunningProcesses().add(ClientApiConverter.toClientApiValue(json));
        }

        u.setUiPreferences(JSONUtil.toJsonNode(user.getUiPreferences()));

        user.getCustomProperties().forEach((k, v) -> u.getProperties().put(k, v.asObjectCopy()));

        Set<String> privileges = privilegeRepository.getPrivileges(user);
        u.getPrivileges().addAll(privileges);

        return u;
    }

    private List<JSONObject> getLongRunningProcesses(User user) {
        return getLongRunningProcessRepository().getLongRunningProcesses(user);
    }

    private LongRunningProcessRepository getLongRunningProcessRepository() {
        if (this.longRunningProcessRepository == null) {
            this.longRunningProcessRepository = InjectHelper.getInstance(LongRunningProcessRepository.class);
        }
        return this.longRunningProcessRepository;
    }

    public ClientApiUser toClientApi(User user) {
        return toClientApi(user, null);
    }

    private ClientApiUser toClientApi(User user, Map<String, String> workspaceNames) {
        ClientApiUser u = new ClientApiUser();
        u.setId(user.getUserId());
        u.setUserName(user.getUsername());
        u.setDisplayName(user.getDisplayName());
        u.setStatus(user.getUserStatus());
        u.setUserType(user.getUserType());
        u.setEmail(user.getEmailAddress());
        u.setCurrentLoginDate(user.getCurrentLoginDate());
        u.setPreviousLoginDate(user.getPreviousLoginDate());
        u.setCurrentWorkspaceId(user.getCurrentWorkspaceId());
        u.getProperties().putAll(user.getCustomProperties());
        if (workspaceNames != null) {
            String workspaceName = workspaceNames.get(user.getCurrentWorkspaceId());
            u.setCurrentWorkspaceName(workspaceName);
        }
        return u;
    }

    protected String formatUsername(String username) {
        return username.trim().toLowerCase();
    }

    public ClientApiUsers toClientApi(Iterable<User> users, Map<String, String> workspaceNames) {
        ClientApiUsers clientApiUsers = new ClientApiUsers();
        for (User user : users) {
            clientApiUsers.getUsers().add(toClientApi(user, workspaceNames));
        }
        return clientApiUsers;
    }

    public static JSONObject toJson(User user) {
        return toJson(user, null);
    }

    public static JSONObject toJson(User user, Map<String, String> workspaceNames) {
        try {
            JSONObject json = new JSONObject();
            json.put("id", user.getUserId());
            json.put("userName", user.getUsername());
            json.put("displayName", user.getDisplayName());
            json.put("status", user.getUserStatus().toString().toUpperCase());
            json.put("userType", user.getUserType().toString().toUpperCase());
            json.put("email", user.getEmailAddress());
            json.put("currentWorkspaceId", user.getCurrentWorkspaceId());
            if (workspaceNames != null) {
                String workspaceName = workspaceNames.get(user.getCurrentWorkspaceId());
                json.put("currentWorkspaceName", workspaceName);
            }
            return json;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public SimpleOrmContext getSimpleOrmContext(User user) {
        Set<Role> roles = authorizationRepository.getRoles(user);
        Set<String> authorizationsSet = roles.stream().map(role -> role.getRoleName()).collect(Collectors.toSet());
        String[] authorizations = authorizationsSet.toArray(new String[authorizationsSet.size()]);
        return getSimpleOrmContext(authorizations);
    }

    public SimpleOrmContext getSimpleOrmContext(String... authorizations) {
        return simpleOrmSession.createContext(authorizations);
    }

    public User getSystemUser() {
        return new SystemUser();
    }

    public User findOrAddUser(
            String username,
            String displayName,
            String emailAddress,
            String password
    ) {
        return lockRepository.lock("findOrAddUser", () -> {
            User user = findByUsername(username);
            if (user == null) {
                user = addUser(username, displayName, emailAddress, password);
            }
            return user;
        });
    }

    public final void delete(User user) {
        internalDelete(user);
        userSessionCounterRepository.deleteSessions(user.getUserId());
        webQueueRepository.broadcastUserStatusChange(user, UserStatus.OFFLINE);
        fireUserDeletedEvent(user);
    }

    protected abstract void internalDelete(User user);

    public Iterable<User> find(String query) {
        final String lowerCaseQuery = query == null ? null : query.toLowerCase();

        int skip = 0;
        int limit = 100;
        List<User> foundUsers = new ArrayList<>();
        while (true) {
            List<User> users = toList(find(skip, limit));
            if (users.size() == 0) {
                break;
            }
            for (User user : users) {
                if (lowerCaseQuery == null
                        || user.getDisplayName().toLowerCase().contains(lowerCaseQuery)
                        || user.getUsername().toLowerCase().contains(lowerCaseQuery)) {
                    foundUsers.add(user);
                }
            }
            skip += limit;
        }
        return foundUsers;
    }

    public static String createRandomPassword() {
        return new BigInteger(120, new SecureRandom()).toString(32);
    }

    public abstract User findByPasswordResetToken(String token);

    public abstract void setPasswordResetTokenAndExpirationDate(User user, String token, ZonedDateTime expirationDate);

    public abstract void clearPasswordResetTokenAndExpirationDate(User user);

    protected void afterNewUserAdded(User newUser) {
        fireNewUserAddedEvent(newUser);
    }

    private void fireNewUserAddedEvent(User user) {
        for (UserListener userListener : getUserListeners()) {
            userListener.newUserAdded(user);
        }
    }

    private void fireUserDeletedEvent(User user) {
        for (UserListener userListener : getUserListeners()) {
            userListener.userDeleted(user);
        }
    }

    protected void fireUserLoginEvent(User user, AuthorizationContext authorizationContext) {
        for (UserListener userListener : getUserListeners()) {
            userListener.userLogin(user, authorizationContext);
        }
    }

    protected void fireUserStatusChangeEvent(User user, UserStatus status) {
        for (UserListener userListener : getUserListeners()) {
            userListener.userStatusChange(user, status);
        }
    }

    protected Collection<UserListener> getUserListeners() {
        if (userListeners == null) {
            userListeners = InjectHelper.getInjectedServices(UserListener.class, configuration);
        }
        return userListeners;
    }

    public abstract void setPropertyOnUser(User user, String propertyName, Value value);

    protected AuthorizationRepository getAuthorizationRepository() {
        return authorizationRepository;
    }

    protected PrivilegeRepository getPrivilegeRepository() {
        return privilegeRepository;
    }

    public void addDefaultAdminUser() {
        User user = findByUsername("admin");
        if (user == null) {
            findOrAddUser(
                    "admin",
                    "App Administrator",
                    "admin@localhost",
                    "admin"
            );
        }
    }

    public void addDefaultSysUser() {
        User user = findByUsername("sys");
        if (user == null) {
            user = findOrAddUser(
                    "sys",
                    "System Administrator",
                    "sys@localhost",
                    "ZnD5QKEWMX867rk"
            );
            authorizationRepository.addRoleToUser(user, authorizationRepository.getAdministratorRole(), new SystemUser());
        }
    }
}
