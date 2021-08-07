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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcResourceNotFoundException;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.lock.LockRepository;
import com.mware.core.model.notification.UserNotificationRepository;
import com.mware.core.model.properties.RoleSchema;
import com.mware.core.model.user.AuthorizationContext;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.trace.Traced;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.query.Compare;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.query.builder.GeQueryBuilders;
import com.mware.ge.query.builder.GeQueryBuilder;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.values.storable.Values;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mware.core.util.StreamUtil.stream;
import static com.mware.ge.util.IterableUtils.singleOrDefault;

public class GeAuthorizationRepository extends AuthorizationRepositoryBase {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(GeAuthorizationRepository.class);
    private static final String GRAPH_ROLE_ID_PREFIX = "ROLE_";
    public static final String VISIBILITY_STRING = "role";
    public static final BcVisibility VISIBILITY = new BcVisibility(VISIBILITY_STRING);
    private com.mware.ge.Authorizations authorizations;
    private final GraphAuthorizationRepository graphAuthorizationRepository;
    private final LockRepository lockRepository;
    private final WebQueueRepository webQueueRepository;

    private String roleConceptId = ROLE_CONCEPT_NAME;

    private final Cache<String, Vertex> roleVertexCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();

    @Inject
    public GeAuthorizationRepository(
            Graph graph,
            GraphAuthorizationRepository graphAuthorizationRepository,
            Configuration configuration,
            UserNotificationRepository userNotificationRepository,
            WebQueueRepository webQueueRepository,
            LockRepository lockRepository
    ) {
        super(graph, userNotificationRepository, webQueueRepository, configuration);

        this.webQueueRepository = webQueueRepository;

        this.graphAuthorizationRepository = graphAuthorizationRepository;
        this.graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);
        this.graphAuthorizationRepository.addAuthorizationToGraph(UserRepository.VISIBILITY_STRING);
        this.graphAuthorizationRepository.addAuthorizationToGraph(BcVisibility.SUPER_USER_VISIBILITY_STRING);
        this.lockRepository = lockRepository;

        Set<String> authorizationsSet = new HashSet<>();
        authorizationsSet.add(VISIBILITY_STRING);
        authorizationsSet.add(UserRepository.VISIBILITY_STRING);
        authorizationsSet.add(BcVisibility.SUPER_USER_VISIBILITY_STRING);
        this.authorizations = graph.createAuthorizations(authorizationsSet);
    }

    @Override
    public void updateUser(User user, AuthorizationContext authorizationContext) {

    }

    @Override
    public Set<Role> getRoles(User user) {
        if (user instanceof SystemUser) {
            return Sets.newHashSet(getAdministratorRole());
        }

        Vertex userVertex = getGraph().getVertex(user.getUserId(), FetchHints.ALL, authorizations);
        return stream(userVertex.getVertices(Direction.IN, RoleSchema.ROLE_TO_USER_RELATIONSHIP_NAME, authorizations))
                .map(GeRole::new)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getRoleNames(User user) {
        return getRoles(user).stream()
                .map(Role::getRoleName)
                .collect(Collectors.toSet());
    }

    @Override
    public Iterable<Role> getAllRoles() {
        try (QueryResultsIterable<Vertex> roleVertices = getGraph().query(GeQueryBuilders.hasConceptType(roleConceptId), authorizations)
                .vertices()) {
            return new ConvertingIterable<Vertex, Role>(roleVertices) {
                @Override
                protected Role convert(Vertex vertex) {
                    return createFromVertex(vertex);
                }
            };
        } catch (Exception e) {
            throw new GeException("Could not close scroll iterable: ", e);
        }
    }

    @Override
    public Role findById(String roleId) {
        return createFromVertex(findByIdRoleVertex(roleId, getGraph().getDefaultFetchHints()));
    }

    @Override
    public Role findByName(String roleName) {
        roleName = formatRole(roleName);
        GeQueryBuilder qb = GeQueryBuilders.boolQuery()
                .and(GeQueryBuilders.hasConceptType(roleConceptId))
                .and(GeQueryBuilders.hasFilter(RoleSchema.ROLE_NAME.getPropertyName(), Compare.EQUAL, Values.stringValue(roleName)));
        Iterable<Vertex> vertices = getGraph().query(qb, authorizations)
                .vertices();
        Vertex roleVertex = singleOrDefault(vertices, null);
        if (roleVertex == null) {
            return null;
        }
        roleVertexCache.put(roleVertex.getId(), roleVertex);
        return createFromVertex(roleVertex);
    }

    private String formatRole(String roleName) {
        return roleName.trim().toLowerCase();
    }

    @Override
    public Role addRole(String roleName, String description, boolean global, Set<Privilege> privileges) {
        roleName = formatRole(roleName);
        description = description.trim();

        String id = GRAPH_ROLE_ID_PREFIX + getGraph().getIdGenerator().nextId();
        VertexBuilder roleBuilder = getGraph().prepareVertex(id, VISIBILITY.getVisibility(), roleConceptId);

        RoleSchema.ROLE_NAME.setProperty(roleBuilder, roleName, VISIBILITY.getVisibility());
        RoleSchema.DESCRIPTION.setProperty(roleBuilder, description, VISIBILITY.getVisibility());
        RoleSchema.GLOBAL.setProperty(roleBuilder, global, VISIBILITY.getVisibility());
        RoleSchema.PRIVILEGES.setProperty(roleBuilder, Privilege.toStringPrivileges(privileges), VISIBILITY.getVisibility());

        Role role = createFromVertex(roleBuilder.save(this.authorizations));
        getGraph().flush();

        fireNewRoleAddedEvent(role);

        return role;
    }

    @Override
    public void deleteRole(Role role) {
        Vertex roleVertex = findByIdRoleVertex(role.getRoleId(), getGraph().getDefaultFetchHints());
        getGraph().softDeleteVertex(roleVertex, authorizations);
        getGraph().flush();
        fireRoleDeletedEvent(role);
    }

    @Override
    public void setRoleName(Role role, String roleName) {
        Vertex roleVertex = findByIdRoleVertex(role.getRoleId(), getGraph().getDefaultFetchHints());
        RoleSchema.ROLE_NAME.setProperty(
                roleVertex,
                roleName,
                VISIBILITY.getVisibility(),
                authorizations
        );
        getGraph().flush();
    }

    @Override
    public void setDescription(Role role, String description) {
        Vertex roleVertex = findByIdRoleVertex(role.getRoleId(), getGraph().getDefaultFetchHints());
        RoleSchema.DESCRIPTION.setProperty(
                roleVertex,
                description,
                VISIBILITY.getVisibility(),
                authorizations
        );
        getGraph().flush();
    }

    @Override
    public void setPrivileges(Role role, Set<Privilege> privileges) {
        Vertex roleVertex = findByIdRoleVertex(role.getRoleId(), getGraph().getDefaultFetchHints());
        String value = Privilege.toStringPrivileges(privileges);
        RoleSchema.PRIVILEGES.setProperty(
                roleVertex,
                value,
                VISIBILITY.getVisibility(),
                authorizations
        );
        getGraph().flush();
    }

    @Override
    public void setGlobal(Role role, boolean global) {
        Vertex roleVertex = findByIdRoleVertex(role.getRoleId(), getGraph().getDefaultFetchHints());
        RoleSchema.GLOBAL.setProperty(
                roleVertex,
                global,
                VISIBILITY.getVisibility(),
                authorizations
        );
        getGraph().flush();
    }

    @Override
    public void addRoleToUser(User user, String roleName, User authUser) {
        Role role = findByName(roleName);
        if(role == null) {
            role = addRole(roleName, "", true, Collections.emptySet());
        }
        addRoleToUser(user, role, authUser);
        getGraph().flush();
    }

    @Override
    public void addRoleToUser(User user, Role role, User authUser) {
        Set<Role> roles = getRoles(user);
        if(!roles.contains(role)) {
            LOGGER.info(
                    "Adding role '%s' to user '%s' by '%s'",
                    role.getRoleName(),
                    user.getUsername(),
                    authUser.getUsername()
            );

            Vertex roleVertex = findByIdRoleVertex(role.getRoleId(), FetchHints.ALL);
            Vertex userVertex = getGraph().getVertex(user.getUserId(), FetchHints.ALL, authorizations);
            EdgeBuilder edgeBuilder = getGraph().prepareEdge(
                    roleVertex,
                    userVertex,
                    RoleSchema.ROLE_TO_USER_RELATIONSHIP_NAME,
                    VISIBILITY.getVisibility()
            );
            edgeBuilder.save(authorizations);
            this.graphAuthorizationRepository.addAuthorizationToGraph(role.getRoleName());
            sendNotificationToUserAboutAddRole(user, role, authUser);
            webQueueRepository.pushUserAccessChange(user);
            fireUserAddRoleEvent(user, role);
        }
    }

    @Override
    public void addRolesToUser(User user, Set<Role> role, User authUser) {
        Set<Role> existingRoles = getRoles(user);
        role.stream()
                .filter(r -> !existingRoles.contains(r))
                .forEach(r -> addRoleToUser(user, r, authUser));
        getGraph().flush();
    }

    @Override
    public void setRolesForUser(User user, Set<Role> newRoles, User authUser) {
        removeAllRolesFromUser(user);

        for(Role role : newRoles) {
            addRoleToUser(user, role, authUser);
        }
        getGraph().flush();
        webQueueRepository.pushUserAccessChange(user);
    }

    private void removeAllRolesFromUser(User user) {
        Set<Role> roles = getRoles(user);
        for(Role role : roles) {
            lockRepository.lock("ROLE_"+role.getRoleId(), () -> {
                Vertex userVertex = getGraph().getVertex(user.getUserId(), FetchHints.ALL, authorizations);
                if (userVertex == null) {
                    throw new BcResourceNotFoundException("Could not find user: " + user.getUserId(), user.getUserId());
                }

                Vertex roleVertex = findByIdRoleVertex(role.getRoleId(), FetchHints.ALL);
                if (roleVertex == null) {
                    throw new BcResourceNotFoundException("Could not find role: " + role.getRoleId(), role.getRoleId());
                }

                List<Edge> edges = stream(roleVertex.getEdges(
                        userVertex,
                        Direction.BOTH,
                        RoleSchema.ROLE_TO_USER_RELATIONSHIP_NAME,
                        authorizations
                )).collect(Collectors.toList());

                for (Edge edge : edges) {
                    getGraph().softDeleteEdge(edge, authorizations);
                }
            });
        }
        getGraph().flush();
    }

    @Override
    public void removeRoleFromUser(User user, Role role, User authUser) {
        Set<Role> roles = getRoles(user);
        if(roles.contains(role)) {
            LOGGER.info(
                    "Removing role '%s' to user '%s' by '%s'",
                    role.getRoleName(),
                    user.getUsername(),
                    authUser.getUsername()
            );


            lockRepository.lock("ROLE_"+role.getRoleId(), () -> {
                Vertex userVertex = getGraph().getVertex(user.getUserId(), FetchHints.ALL, authorizations);
                if (userVertex == null) {
                    throw new BcResourceNotFoundException("Could not find user: " + user.getUserId(), user.getUserId());
                }

                Vertex roleVertex = findByIdRoleVertex(role.getRoleId(), FetchHints.ALL);
                if (roleVertex == null) {
                    throw new BcResourceNotFoundException("Could not find role: " + role.getRoleId(), role.getRoleId());
                }

                List<Edge> edges = stream(roleVertex.getEdges(
                        userVertex,
                        Direction.BOTH,
                        RoleSchema.ROLE_TO_USER_RELATIONSHIP_NAME,
                        authorizations
                )).collect(Collectors.toList());

                for (Edge edge : edges) {
                    getGraph().softDeleteEdge(edge, authorizations);
                }
                getGraph().flush();
            });

            sendNotificationToUserAboutRemoveRole(user, role, authUser);
            webQueueRepository.pushUserAccessChange(user);
            fireUserRemoveRoleEvent(user, role);
        }
    }

    private GeRole createFromVertex(Vertex role) {
        if (role == null) {
            return null;
        }

        return new GeRole(role);
    }

    @Traced
    private Vertex findByIdRoleVertex(String roleId, FetchHints fetchHints) {
        Vertex roleVertex = roleVertexCache.getIfPresent(roleId);
        if (roleVertex != null) {
            return roleVertex;
        }
        roleVertex = getGraph().getVertex(roleId, fetchHints, authorizations);
        if (roleVertex != null) {
            roleVertexCache.put(roleId, roleVertex);
        }
        return roleVertex;
    }
}
