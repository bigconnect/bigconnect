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

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.UserStatus;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.ge.values.storable.Value;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Singleton
public class InMemoryUserRepository extends UserRepository {
    private List<User> users = new ArrayList<>();

    @Inject
    public InMemoryUserRepository(
            Configuration configuration,
            SimpleOrmSession simpleOrmSession,
            UserSessionCounterRepository userSessionCounterRepository,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            LockRepository lockRepository,
            AuthorizationRepository authorizationRepository,
            PrivilegeRepository privilegeRepository
    ) {
        super(
                configuration,
                simpleOrmSession,
                userSessionCounterRepository,
                workQueueRepository,
                webQueueRepository,
                lockRepository,
                authorizationRepository,
                privilegeRepository
        );

        addDefaultAdminUser();
        addDefaultSysUser();
    }

    @Override
    public User findByUsername(final String username) {
        return Iterables.find(this.users, user -> user.getUsername().equals(username), null);
    }

    @Override
    public Iterable<User> find(int skip, int limit) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public User findById(final String userId) {
        return Iterables.find(this.users, user -> user.getUserId().equals(userId), null);
    }

    @Override
    public User addUser(
            String username,
            String displayName,
            String emailAddress,
            String password
    ) {
        username = formatUsername(username);
        displayName = displayName.trim();
        InMemoryUser user = new InMemoryUser(
                username,
                displayName,
                emailAddress,
                null
        );
        afterNewUserAdded(user);
        users.add(user);
        return user;
    }

    @Override
    public void setPassword(User user, byte[] salt, byte[] passwordHash) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean isPasswordValid(User user, String password) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateUser(User user, AuthorizationContext authorizationContext) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public User setCurrentWorkspace(String userId, String workspaceId) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public String getCurrentWorkspaceId(String userId) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public User setStatus(String userId, UserStatus status) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void setUiPreferences(User user, JSONObject preferences) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void setDisplayName(User user, String displayName) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void setEmailAddress(User user, String emailAddress) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    protected void internalDelete(User user) {

    }

    @Override
    public User findByPasswordResetToken(String token) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void setPasswordResetTokenAndExpirationDate(User user, String token, ZonedDateTime expirationDate) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void clearPasswordResetTokenAndExpirationDate(User user) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void setPropertyOnUser(User user, String propertyName, Value value) {
        if (user instanceof SystemUser) {
            throw new BcException("Cannot set properties on system user");
        }
        ((InMemoryUser) user).setProperty(propertyName, value);
    }
}
