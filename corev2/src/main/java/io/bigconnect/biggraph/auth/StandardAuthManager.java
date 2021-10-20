/*
 * Copyright 2021 BigConnect Authors
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.bigconnect.biggraph.auth;

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.BigGraphParams;
import io.bigconnect.biggraph.auth.BigUser.P;
import io.bigconnect.biggraph.auth.SchemaDefine.AuthElement;
import io.bigconnect.biggraph.backend.cache.Cache;
import io.bigconnect.biggraph.backend.cache.CacheManager;
import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.config.AuthOptions;
import io.bigconnect.biggraph.config.BigConfig;
import io.bigconnect.biggraph.event.EventListener;
import io.bigconnect.biggraph.exception.NotAllowException;
import io.bigconnect.biggraph.type.define.Directions;
import io.bigconnect.biggraph.util.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.jsonwebtoken.Claims;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import javax.security.sasl.AuthenticationException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

public class StandardAuthManager implements AuthManager {

    protected static final Logger LOG = Log.logger(StandardAuthManager.class);

    private final BigGraphParams graph;
    private final EventListener eventListener;

    // Cache <username, HugeUser>
    private final Cache<Id, BigUser> usersCache;
    // Cache <userId, passwd>
    private final Cache<Id, String> pwdCache;
    // Cache <token, username>
    private final Cache<Id, String> tokenCache;

    private final EntityManager<BigUser> users;
    private final EntityManager<BigGroup> groups;
    private final EntityManager<BigTarget> targets;
    private final EntityManager<BigProject> project;

    private final RelationshipManager<BigBelong> belong;
    private final RelationshipManager<BigAccess> access;

    private final TokenGenerator tokenGenerator;
    private final long tokenExpire;

    public StandardAuthManager(BigGraphParams graph) {
        E.checkNotNull(graph, "graph");
        BigConfig config = graph.configuration();
        long expired = config.get(AuthOptions.AUTH_CACHE_EXPIRE);
        long capacity = config.get(AuthOptions.AUTH_CACHE_CAPACITY);
        this.tokenExpire = config.get(AuthOptions.AUTH_TOKEN_EXPIRE);

        this.graph = graph;
        this.eventListener = this.listenChanges();
        this.usersCache = this.cache("users", capacity, expired);
        this.pwdCache = this.cache("users_pwd", capacity, expired);
        this.tokenCache = this.cache("token", capacity, expired);

        this.users = new EntityManager<>(this.graph, BigUser.P.USER,
                                         BigUser::fromVertex);
        this.groups = new EntityManager<>(this.graph, BigGroup.P.GROUP,
                                          BigGroup::fromVertex);
        this.targets = new EntityManager<>(this.graph, BigTarget.P.TARGET,
                                           BigTarget::fromVertex);
        this.project = new EntityManager<>(this.graph, BigProject.P.PROJECT,
                                           BigProject::fromVertex);

        this.belong = new RelationshipManager<>(this.graph, BigBelong.P.BELONG,
                                                BigBelong::fromEdge);
        this.access = new RelationshipManager<>(this.graph, BigAccess.P.ACCESS,
                                                BigAccess::fromEdge);

        this.tokenGenerator = new TokenGenerator(config);
    }

    private <V> Cache<Id, V> cache(String prefix, long capacity,
                                   long expiredTime) {
        String name = prefix + "-" + this.graph.name();
        Cache<Id, V> cache = CacheManager.instance().cache(name, capacity);
        if (expiredTime > 0L) {
            cache.expire(Duration.ofSeconds(expiredTime).toMillis());
        } else {
            cache.expire(expiredTime);
        }
        return cache;
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure user schema create after system info initialized
            if (storeEvents.contains(event.name())) {
                try {
                    this.initSchemaIfNeeded();
                } finally {
                    this.graph.closeTx();
                }
                return true;
            }
            return false;
        };
        this.graph.loadSystemStore().provider().listen(eventListener);
        return eventListener;
    }

    private void unlistenChanges() {
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
    }

    @Override
    public boolean close() {
        this.unlistenChanges();
        return true;
    }

    private void initSchemaIfNeeded() {
        this.invalidateUserCache();
        BigUser.schema(this.graph).initSchemaIfNeeded();
        BigGroup.schema(this.graph).initSchemaIfNeeded();
        BigTarget.schema(this.graph).initSchemaIfNeeded();
        BigBelong.schema(this.graph).initSchemaIfNeeded();
        BigAccess.schema(this.graph).initSchemaIfNeeded();
        BigProject.schema(this.graph).initSchemaIfNeeded();
    }

    private void invalidateUserCache() {
        this.usersCache.clear();
    }

    private void invalidatePasswordCache(Id id) {
        this.pwdCache.invalidate(id);
        // Clear all tokenCache because can't get userId in it
        this.tokenCache.clear();
    }

    @Override
    public Id createUser(BigUser user) {
        this.invalidateUserCache();
        return this.users.add(user);
    }

    @Override
    public Id updateUser(BigUser user) {
        this.invalidateUserCache();
        this.invalidatePasswordCache(user.id());
        return this.users.update(user);
    }

    @Override
    public BigUser deleteUser(Id id) {
        this.invalidateUserCache();
        this.invalidatePasswordCache(id);
        return this.users.delete(id);
    }

    @Override
    public BigUser findUser(String name) {
        Id username = IdGenerator.of(name);
        BigUser user = this.usersCache.get(username);
        if (user != null) {
            return user;
        }

        List<BigUser> users = this.users.query(P.NAME, name, 2L);
        if (users.size() > 0) {
            assert users.size() == 1;
            user = users.get(0);
            this.usersCache.update(username, user);
        }
        return user;
    }

    @Override
    public BigUser getUser(Id id) {
        return this.users.get(id);
    }

    @Override
    public List<BigUser> listUsers(List<Id> ids) {
        return this.users.list(ids);
    }

    @Override
    public List<BigUser> listAllUsers(long limit) {
        return this.users.list(limit);
    }

    @Override
    public Id createGroup(BigGroup group) {
        this.invalidateUserCache();
        return this.groups.add(group);
    }

    @Override
    public Id updateGroup(BigGroup group) {
        this.invalidateUserCache();
        return this.groups.update(group);
    }

    @Override
    public BigGroup deleteGroup(Id id) {
        this.invalidateUserCache();
        return this.groups.delete(id);
    }

    @Override
    public BigGroup getGroup(Id id) {
        return this.groups.get(id);
    }

    @Override
    public List<BigGroup> listGroups(List<Id> ids) {
        return this.groups.list(ids);
    }

    @Override
    public List<BigGroup> listAllGroups(long limit) {
        return this.groups.list(limit);
    }

    @Override
    public Id createTarget(BigTarget target) {
        this.invalidateUserCache();
        return this.targets.add(target);
    }

    @Override
    public Id updateTarget(BigTarget target) {
        this.invalidateUserCache();
        return this.targets.update(target);
    }

    @Override
    public BigTarget deleteTarget(Id id) {
        this.invalidateUserCache();
        return this.targets.delete(id);
    }

    @Override
    public BigTarget getTarget(Id id) {
        return this.targets.get(id);
    }

    @Override
    public List<BigTarget> listTargets(List<Id> ids) {
        return this.targets.list(ids);
    }

    @Override
    public List<BigTarget> listAllTargets(long limit) {
        return this.targets.list(limit);
    }

    @Override
    public Id createBelong(BigBelong belong) {
        this.invalidateUserCache();
        E.checkArgument(this.users.exists(belong.source()),
                        "Not exists user '%s'", belong.source());
        E.checkArgument(this.groups.exists(belong.target()),
                        "Not exists group '%s'", belong.target());
        return this.belong.add(belong);
    }

    @Override
    public Id updateBelong(BigBelong belong) {
        this.invalidateUserCache();
        return this.belong.update(belong);
    }

    @Override
    public BigBelong deleteBelong(Id id) {
        this.invalidateUserCache();
        return this.belong.delete(id);
    }

    @Override
    public BigBelong getBelong(Id id) {
        return this.belong.get(id);
    }

    @Override
    public List<BigBelong> listBelong(List<Id> ids) {
        return this.belong.list(ids);
    }

    @Override
    public List<BigBelong> listAllBelong(long limit) {
        return this.belong.list(limit);
    }

    @Override
    public List<BigBelong> listBelongByUser(Id user, long limit) {
        return this.belong.list(user, Directions.OUT,
                                BigBelong.P.BELONG, limit);
    }

    @Override
    public List<BigBelong> listBelongByGroup(Id group, long limit) {
        return this.belong.list(group, Directions.IN,
                                BigBelong.P.BELONG, limit);
    }

    @Override
    public Id createAccess(BigAccess access) {
        this.invalidateUserCache();
        E.checkArgument(this.groups.exists(access.source()),
                        "Not exists group '%s'", access.source());
        E.checkArgument(this.targets.exists(access.target()),
                        "Not exists target '%s'", access.target());
        return this.access.add(access);
    }

    @Override
    public Id updateAccess(BigAccess access) {
        this.invalidateUserCache();
        return this.access.update(access);
    }

    @Override
    public BigAccess deleteAccess(Id id) {
        this.invalidateUserCache();
        return this.access.delete(id);
    }

    @Override
    public BigAccess getAccess(Id id) {
        return this.access.get(id);
    }

    @Override
    public List<BigAccess> listAccess(List<Id> ids) {
        return this.access.list(ids);
    }

    @Override
    public List<BigAccess> listAllAccess(long limit) {
        return this.access.list(limit);
    }

    @Override
    public List<BigAccess> listAccessByGroup(Id group, long limit) {
        return this.access.list(group, Directions.OUT,
                                BigAccess.P.ACCESS, limit);
    }

    @Override
    public List<BigAccess> listAccessByTarget(Id target, long limit) {
        return this.access.list(target, Directions.IN,
                                BigAccess.P.ACCESS, limit);
    }

    @Override
    public Id createProject(BigProject project) {
        E.checkArgument(!StringUtils.isEmpty(project.name()),
                        "The name of project can't be null or empty");
        return commit(() -> {
            // Create project admin group
            if (project.adminGroupId() == null) {
                BigGroup adminGroup = new BigGroup("admin_" + project.name());
                /*
                 * "creator" is a necessary parameter, other places are passed
                 * in "AuthManagerProxy", but here is the underlying module, so
                 * pass it directly here
                 */
                adminGroup.creator(project.creator());
                Id adminGroupId = this.createGroup(adminGroup);
                project.adminGroupId(adminGroupId);
            }

            // Create project op group
            if (project.opGroupId() == null) {
                BigGroup opGroup = new BigGroup("op_" + project.name());
                // Ditto
                opGroup.creator(project.creator());
                Id opGroupId = this.createGroup(opGroup);
                project.opGroupId(opGroupId);
            }

            // Create project target to verify permission
            final String targetName = "project_res_" + project.name();
            BigResource resource = new BigResource(ResourceType.PROJECT,
                                                     project.name(),
                                                     null);
            BigTarget target = new BigTarget(targetName,
                                               this.graph.name(),
                                               "localhost:8080",
                                               ImmutableList.of(resource));
            // Ditto
            target.creator(project.creator());
            Id targetId = this.targets.add(target);
            project.targetId(targetId);

            Id adminGroupId = project.adminGroupId();
            Id opGroupId = project.opGroupId();
            BigAccess adminGroupWriteAccess = new BigAccess(
                                                   adminGroupId, targetId,
                                                   BigPermission.WRITE);
            // Ditto
            adminGroupWriteAccess.creator(project.creator());
            BigAccess adminGroupReadAccess = new BigAccess(
                                                  adminGroupId, targetId,
                                                  BigPermission.READ);
            // Ditto
            adminGroupReadAccess.creator(project.creator());
            BigAccess opGroupReadAccess = new BigAccess(opGroupId, targetId,
                                                          BigPermission.READ);
            // Ditto
            opGroupReadAccess.creator(project.creator());
            this.access.add(adminGroupWriteAccess);
            this.access.add(adminGroupReadAccess);
            this.access.add(opGroupReadAccess);
            return this.project.add(project);
        });
    }

    @Override
    public BigProject deleteProject(Id id) {
        return this.commit(() -> {
            LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
            try {
                locks.lockWrites(LockUtil.PROJECT_UPDATE, id);

                BigProject oldProject = this.project.get(id);
                /*
                 * Check whether there are any graph binding this project,
                 * throw ForbiddenException, if it is
                 */
                if (!CollectionUtils.isEmpty(oldProject.graphs())) {
                    String errInfo = String.format("Can't delete project '%s' " +
                                                   "that contains any graph, " +
                                                   "there are graphs bound " +
                                                   "to it", id);
                    throw new NotAllowException(errInfo);
                }
                BigProject project = this.project.delete(id);
                E.checkArgumentNotNull(project,
                                       "Failed to delete the project '%s'",
                                       id);
                E.checkArgumentNotNull(project.adminGroupId(),
                                       "Failed to delete the project '%s'," +
                                       "the admin group of project can't " +
                                       "be null", id);
                E.checkArgumentNotNull(project.opGroupId(),
                                       "Failed to delete the project '%s'," +
                                       "the op group of project can't be null",
                                       id);
                E.checkArgumentNotNull(project.targetId(),
                                       "Failed to delete the project '%s', " +
                                       "the target resource of project " +
                                       "can't be null", id);
                // Delete admin group
                this.groups.delete(project.adminGroupId());
                // Delete op group
                this.groups.delete(project.opGroupId());
                // Delete project_target
                this.targets.delete(project.targetId());
                return project;
            } finally {
                locks.unlock();
            }
        });
    }

    @Override
    public Id updateProject(BigProject project) {
        return this.project.update(project);
    }

    @Override
    public Id projectAddGraphs(Id id, Set<String> graphs) {
        E.checkArgument(!CollectionUtils.isEmpty(graphs),
                        "Failed to add graphs to project '%s', the graphs " +
                        "parameter can't be empty", id);

        LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
        try {
            locks.lockWrites(LockUtil.PROJECT_UPDATE, id);

            BigProject project = this.project.get(id);
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            int oldSize = sourceGraphs.size();
            sourceGraphs.addAll(graphs);
            // Return if there is none graph been added
            if (sourceGraphs.size() == oldSize) {
                return id;
            }
            project.graphs(sourceGraphs);
            return this.project.update(project);
        } finally {
            locks.unlock();
        }
    }

    @Override
    public Id projectRemoveGraphs(Id id, Set<String> graphs) {
        E.checkArgumentNotNull(id,
                               "Failed to remove graphs, the project id " +
                               "parameter can't be null");
        E.checkArgument(!CollectionUtils.isEmpty(graphs),
                        "Failed to delete graphs from the project '%s', " +
                        "the graphs parameter can't be null or empty", id);

        LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
        try {
            locks.lockWrites(LockUtil.PROJECT_UPDATE, id);

            BigProject project = this.project.get(id);
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            int oldSize = sourceGraphs.size();
            sourceGraphs.removeAll(graphs);
            // Return if there is none graph been removed
            if (sourceGraphs.size() == oldSize) {
                return id;
            }
            project.graphs(sourceGraphs);
            return this.project.update(project);
        } finally {
            locks.unlock();
        }
    }

    @Override
    public BigProject getProject(Id id) {
        return this.project.get(id);
    }

    @Override
    public List<BigProject> listAllProject(long limit) {
        return this.project.list(limit);
    }

    @Override
    public BigUser matchUser(String name, String password) {
        E.checkArgumentNotNull(name, "User name can't be null");
        E.checkArgumentNotNull(password, "User password can't be null");

        BigUser user = this.findUser(name);
        if (user == null) {
            return null;
        }

        if (password.equals(this.pwdCache.get(user.id()))) {
            return user;
        }

        if (StringEncoding.checkPassword(password, user.password())) {
            this.pwdCache.update(user.id(), password);
            return user;
        }
        return null;
    }

    @Override
    public RolePermission rolePermission(AuthElement element) {
        if (element instanceof BigUser) {
            return this.rolePermission((BigUser) element);
        } else if (element instanceof BigTarget) {
            return this.rolePermission((BigTarget) element);
        }

        List<BigAccess> accesses = new ArrayList<>();
        if (element instanceof BigBelong) {
            BigBelong belong = (BigBelong) element;
            accesses.addAll(this.listAccessByGroup(belong.target(), -1));
        } else if (element instanceof BigGroup) {
            BigGroup group = (BigGroup) element;
            accesses.addAll(this.listAccessByGroup(group.id(), -1));
        } else if (element instanceof BigAccess) {
            BigAccess access = (BigAccess) element;
            accesses.add(access);
        } else {
            E.checkArgument(false, "Invalid type for role permission: %s",
                            element);
        }

        return this.rolePermission(accesses);
    }

    private RolePermission rolePermission(BigUser user) {
        if (user.role() != null) {
            // Return cached role (40ms => 10ms)
            return user.role();
        }

        // Collect accesses by user
        List<BigAccess> accesses = new ArrayList<>();
        List<BigBelong> belongs = this.listBelongByUser(user.id(), -1);
        for (BigBelong belong : belongs) {
            accesses.addAll(this.listAccessByGroup(belong.target(), -1));
        }

        // Collect permissions by accesses
        RolePermission role = this.rolePermission(accesses);

        user.role(role);
        return role;
    }

    private RolePermission rolePermission(List<BigAccess> accesses) {
        // Mapping of: graph -> action -> resource
        RolePermission role = new RolePermission();
        for (BigAccess access : accesses) {
            BigPermission accessPerm = access.permission();
            BigTarget target = this.getTarget(access.target());
            role.add(target.graph(), accessPerm, target.resources());
        }
        return role;
    }

    private RolePermission rolePermission(BigTarget target) {
        RolePermission role = new RolePermission();
        // TODO: improve for the actual meaning
        role.add(target.graph(), BigPermission.READ, target.resources());
        return role;
    }

    @Override
    public String loginUser(String username, String password)
                            throws AuthenticationException {
        BigUser user = this.matchUser(username, password);
        if (user == null) {
            String msg = "Incorrect username or password";
            throw new AuthenticationException(msg);
        }

        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                 username,
                                                 AuthConstant.TOKEN_USER_ID,
                                                 user.id.asString());
        String token = this.tokenGenerator.create(payload, this.tokenExpire);

        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
    }

    @Override
    public void logoutUser(String token) {
        this.tokenCache.invalidate(IdGenerator.of(token));
    }

    @Override
    public UserWithRole validateUser(String username, String password) {
        BigUser user = this.matchUser(username, password);
        if (user == null) {
            return new UserWithRole(username);
        }
        return new UserWithRole(user.id, username, this.rolePermission(user));
    }

    @Override
    public UserWithRole validateUser(String token) {
        String username = this.tokenCache.get(IdGenerator.of(token));

        Claims payload = null;
        boolean needBuildCache = false;
        if (username == null) {
            payload = this.tokenGenerator.verify(token);
            username = (String) payload.get(AuthConstant.TOKEN_USER_NAME);
            needBuildCache = true;
        }

        BigUser user = this.findUser(username);
        if (user == null) {
            return new UserWithRole(username);
        } else if (needBuildCache) {
            long expireAt = payload.getExpiration().getTime();
            long bornTime = this.tokenCache.expire() -
                            (expireAt - System.currentTimeMillis());
            this.tokenCache.update(IdGenerator.of(token), username,
                                   Math.negateExact(bornTime));
        }

        return new UserWithRole(user.id(), username, this.rolePermission(user));
    }

    /**
     * Maybe can define an proxy class to choose forward or call local
     */
    public static boolean isLocal(AuthManager authManager) {
        return authManager instanceof StandardAuthManager;
    }

    public <R> R commit(Callable<R> callable) {
        this.groups.autoCommit(false);
        this.access.autoCommit(false);
        this.targets.autoCommit(false);
        this.project.autoCommit(false);
        this.belong.autoCommit(false);
        this.users.autoCommit(false);

        try {
            R result = callable.call();
            this.graph.systemTransaction().commit();
            return result;
        } catch (Throwable e) {
            this.groups.autoCommit(true);
            this.access.autoCommit(true);
            this.targets.autoCommit(true);
            this.project.autoCommit(true);
            this.belong.autoCommit(true);
            this.users.autoCommit(true);
            try {
                this.graph.systemTransaction().rollback();
            } catch (Throwable rollbackException) {
                LOG.error("Failed to rollback transaction: {}",
                          rollbackException.getMessage(), rollbackException);
            }
            if (e instanceof BigGraphException) {
                throw (BigGraphException) e;
            } else {
                throw new BigGraphException("Failed to commit transaction: %s",
                                        e.getMessage(), e);
            }
        }
    }
}
