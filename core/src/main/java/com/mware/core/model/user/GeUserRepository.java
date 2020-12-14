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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.UserStatus;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.properties.UserSchema;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.core.security.BcVisibility;
import com.mware.core.user.ProxyUser;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.ge.util.IterableUtils.singleOrDefault;

@Singleton
public class GeUserRepository extends UserRepository {
    private Graph graph;
    private String userConceptId;
    private com.mware.ge.Authorizations authorizations;
    private final Cache<String, Vertex> userVertexCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();

    @Inject
    public GeUserRepository(
            Configuration configuration,
            SimpleOrmSession simpleOrmSession,
            GraphAuthorizationRepository graphAuthorizationRepository,
            Graph graph,
            SchemaRepository schemaRepository,
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
        this.graph = graph;

        graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);
        graphAuthorizationRepository.addAuthorizationToGraph(BcVisibility.SUPER_USER_VISIBILITY_STRING);

        Concept userConcept = schemaRepository.getConceptByName(USER_CONCEPT_NAME, SchemaRepository.PUBLIC);
        userConceptId = userConcept.getName();

        Set<String> authorizationsSet = new HashSet<>();
        authorizationsSet.add(VISIBILITY_STRING);
        authorizationsSet.add(BcVisibility.SUPER_USER_VISIBILITY_STRING);
        this.authorizations = graph.createAuthorizations(authorizationsSet);
    }

    private GeUser createFromVertex(Vertex user) {
        if (user == null) {
            return null;
        }

        return new GeUser(user);
    }

    @Override
    public User findByUsername(String username) {
        username = formatUsername(username);
        Iterable<Vertex> vertices = graph.query(authorizations)
                .has(UserSchema.USERNAME.getPropertyName(), Values.stringValue(username))
                .hasConceptType(userConceptId)
                .vertices();
        Vertex userVertex = singleOrDefault(vertices, null);
        if (userVertex == null) {
            return null;
        }
        userVertexCache.put(userVertex.getId(), userVertex);
        return createFromVertex(userVertex);
    }

    @Override
    public Iterable<User> find(int skip, int limit) {
        try (QueryResultsIterable<Vertex> userVertices = graph.query(authorizations)
                .hasConceptType(userConceptId)
                .skip(skip)
                .limit(limit)
                .vertices()) {
            return new ConvertingIterable<Vertex, User>(userVertices) {
                @Override
                protected User convert(Vertex vertex) {
                    return createFromVertex(vertex);
                }
            };
        } catch (Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public Iterable<User> findByStatus(int skip, int limit, UserStatus status) {
        try (QueryResultsIterable<Vertex> userVertices = graph.query(authorizations)
                .hasConceptType(userConceptId)
                .has(UserSchema.STATUS.getPropertyName(), Values.stringValue(status.toString()))
                .skip(skip)
                .limit(limit)
                .vertices()) {
            return new ConvertingIterable<Vertex, User>(userVertices) {
                @Override
                protected User convert(Vertex vertex) {
                    return createFromVertex(vertex);
                }
            };
        } catch (Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public User findById(String userId) {
        if (SystemUser.USER_ID.equals(userId)) {
            return getSystemUser();
        }
        return createFromVertex(findByIdUserVertex(userId));
    }

    public Vertex findByIdUserVertex(String userId) {
        Vertex userVertex = userVertexCache.getIfPresent(userId);
        if (userVertex != null) {
            return userVertex;
        }
        userVertex = graph.getVertex(userId, FetchHints.ALL, authorizations);
        if (userVertex != null) {
            userVertexCache.put(userId, userVertex);
        }
        return userVertex;
    }

    @Override
    public User addUser(String username, String displayName, String emailAddress, String password) {
        username = formatUsername(username);
        displayName = displayName.trim();

        byte[] salt = UserPasswordUtil.getSalt();
        byte[] passwordHash = UserPasswordUtil.hashPassword(password, salt);

        String id = GRAPH_USER_ID_PREFIX + graph.getIdGenerator().nextId();
        VertexBuilder userBuilder = graph.prepareVertex(id, VISIBILITY.getVisibility(), userConceptId);

        UserSchema.USERNAME.setProperty(userBuilder, username, VISIBILITY.getVisibility());
        UserSchema.DISPLAY_NAME.setProperty(userBuilder, displayName, VISIBILITY.getVisibility());
        UserSchema.CREATE_DATE.setProperty(userBuilder, ZonedDateTime.now(), VISIBILITY.getVisibility());
        UserSchema.PASSWORD_SALT.setProperty(userBuilder, salt, VISIBILITY.getVisibility());
        UserSchema.PASSWORD_HASH.setProperty(userBuilder, passwordHash, VISIBILITY.getVisibility());
        UserSchema.STATUS.setProperty(
                userBuilder,
                UserStatus.OFFLINE.toString(),
                VISIBILITY.getVisibility()
        );

        if (emailAddress != null) {
            UserSchema.EMAIL_ADDRESS.setProperty(userBuilder, emailAddress, VISIBILITY.getVisibility());
        }

        User user = createFromVertex(userBuilder.save(this.authorizations));
        graph.flush();

        afterNewUserAdded(user);

        return user;
    }

    @Override
    public void setPassword(User user, byte[] salt, byte[] passwordHash) {
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        UserSchema.PASSWORD_SALT.setProperty(userVertex, salt, VISIBILITY.getVisibility(), authorizations);
        UserSchema.PASSWORD_HASH.setProperty(
                userVertex,
                passwordHash,
                VISIBILITY.getVisibility(),
                authorizations
        );
        graph.flush();
    }

    @Override
    public boolean isPasswordValid(User user, String password) {
        try {
            Vertex userVertex = findByIdUserVertex(user.getUserId());
            return UserPasswordUtil.validatePassword(
                    password,
                    UserSchema.PASSWORD_SALT.getPropertyValue(userVertex),
                    UserSchema.PASSWORD_HASH.getPropertyValue(userVertex)
            );
        } catch (Exception ex) {
            throw new RuntimeException("error validating password", ex);
        }
    }

    @Override
    public void updateUser(User user, AuthorizationContext authorizationContext) {
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        ExistingElementMutation<Vertex> m = userVertex.prepareMutation();

        ZonedDateTime currentLoginDate = UserSchema.CURRENT_LOGIN_DATE.getPropertyValue(userVertex);
        if (currentLoginDate != null) {
            UserSchema.PREVIOUS_LOGIN_DATE.setProperty(m, currentLoginDate, VISIBILITY.getVisibility());
        }

        String currentLoginRemoteAddr = UserSchema.CURRENT_LOGIN_REMOTE_ADDR.getPropertyValue(userVertex);
        if (currentLoginRemoteAddr != null) {
            UserSchema.PREVIOUS_LOGIN_REMOTE_ADDR.setProperty(
                    m,
                    currentLoginRemoteAddr,
                    VISIBILITY.getVisibility()
            );
        }

        UserSchema.CURRENT_LOGIN_DATE.setProperty(m, ZonedDateTime.now(), VISIBILITY.getVisibility());
        UserSchema.CURRENT_LOGIN_REMOTE_ADDR.setProperty(
                m,
                authorizationContext.getRemoteAddr(),
                VISIBILITY.getVisibility()
        );

        int loginCount = UserSchema.LOGIN_COUNT.getPropertyValue(userVertex, 0);
        UserSchema.LOGIN_COUNT.setProperty(m, loginCount + 1, VISIBILITY.getVisibility());

        m.save(authorizations);
        graph.flush();

        getAuthorizationRepository().updateUser(user, authorizationContext);
        getPrivilegeRepository().updateUser(user, authorizationContext);
        fireUserLoginEvent(user, authorizationContext);
    }

    @Override
    public User setCurrentWorkspace(String userId, String workspaceId) {
        User user = findById(userId);
        checkNotNull(user, "Could not find user: " + userId);
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        if (workspaceId == null) {
            UserSchema.CURRENT_WORKSPACE.removeProperty(userVertex, authorizations);
        } else {
            UserSchema.CURRENT_WORKSPACE.setProperty(
                    userVertex,
                    workspaceId,
                    VISIBILITY.getVisibility(),
                    authorizations
            );
        }
        graph.flush();
        return user;
    }

    @Override
    public String getCurrentWorkspaceId(String userId) {
        User user = findById(userId);
        checkNotNull(user, "Could not find user: " + userId);
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        return UserSchema.CURRENT_WORKSPACE.getPropertyValue(userVertex);
    }

    @Override
    public void setUiPreferences(User user, JSONObject preferences) {
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        UserSchema.UI_PREFERENCES.setProperty(
                userVertex,
                preferences,
                VISIBILITY.getVisibility(),
                authorizations
        );
        graph.flush();
    }

    @Override
    public User setStatus(String userId, UserStatus status) {
        GeUser user = (GeUser) findById(userId);
        checkNotNull(user, "Could not find user: " + userId);
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        UserSchema.STATUS.setProperty(
                userVertex,
                status.toString(),
                VISIBILITY.getVisibility(),
                authorizations
        );
        graph.flush();
        user.setUserStatus(status);
        fireUserStatusChangeEvent(user, status);
        return user;
    }

    @Override
    public void setDisplayName(User user, String displayName) {
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        UserSchema.DISPLAY_NAME.setProperty(
                userVertex,
                displayName,
                VISIBILITY.getVisibility(),
                authorizations
        );
        graph.flush();
    }

    @Override
    public void setEmailAddress(User user, String emailAddress) {
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        UserSchema.EMAIL_ADDRESS.setProperty(
                userVertex,
                emailAddress,
                VISIBILITY.getVisibility(),
                authorizations
        );
        graph.flush();
    }

    @Override
    protected void internalDelete(User user) {
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        graph.softDeleteVertex(userVertex, authorizations);
        graph.flush();
    }

    @Override
    public User findByPasswordResetToken(String token) {
        try (QueryResultsIterable<Vertex> userVertices = graph.query(authorizations)
                .has(UserSchema.PASSWORD_RESET_TOKEN.getPropertyName(), Values.stringValue(token))
                .hasConceptType(userConceptId)
                .vertices()) {
            Vertex user = singleOrDefault(userVertices, null);
            return createFromVertex(user);
        } catch (Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public void setPasswordResetTokenAndExpirationDate(User user, String token, ZonedDateTime expirationDate) {
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        UserSchema.PASSWORD_RESET_TOKEN.setProperty(
                userVertex,
                token,
                VISIBILITY.getVisibility(),
                authorizations
        );
        UserSchema.PASSWORD_RESET_TOKEN_EXPIRATION_DATE.setProperty(
                userVertex,
                expirationDate,
                VISIBILITY.getVisibility(),
                authorizations
        );
        graph.flush();
    }

    @Override
    public void clearPasswordResetTokenAndExpirationDate(User user) {
        Vertex userVertex = findByIdUserVertex(user.getUserId());
        UserSchema.PASSWORD_RESET_TOKEN.removeProperty(userVertex, authorizations);
        UserSchema.PASSWORD_RESET_TOKEN_EXPIRATION_DATE.removeProperty(userVertex, authorizations);
        graph.flush();
    }

    @Override
    public void setPropertyOnUser(User user, String propertyName, Value value) {
        if (user instanceof SystemUser) {
            throw new BcException("Cannot set properties on system user");
        }
        if (user.getCustomProperties().get(propertyName) == null || !value.equals(user.getCustomProperties().get(propertyName))) {
            Vertex userVertex = findByIdUserVertex(user.getUserId());
            userVertex.setProperty(propertyName, value, VISIBILITY.getVisibility(), authorizations);
            if (user instanceof ProxyUser){
                User proxiedUser = ((ProxyUser) user).getProxiedUser();
                if (proxiedUser instanceof GeUser) {
                    user = proxiedUser;
                }
            }
            if (user instanceof GeUser) {
                ((GeUser) user).setProperty(propertyName, value);
            }
            graph.flush();
        }
    }
}
