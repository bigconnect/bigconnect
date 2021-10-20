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

import io.bigconnect.biggraph.auth.SchemaDefine.AuthElement;
import io.bigconnect.biggraph.backend.id.Id;

import javax.security.sasl.AuthenticationException;
import java.util.List;
import java.util.Set;

public interface AuthManager {

    public boolean close();

    public Id createUser(BigUser user);
    public Id updateUser(BigUser user);
    public BigUser deleteUser(Id id);
    public BigUser findUser(String name);
    public BigUser getUser(Id id);
    public List<BigUser> listUsers(List<Id> ids);
    public List<BigUser> listAllUsers(long limit);

    public Id createGroup(BigGroup group);
    public Id updateGroup(BigGroup group);
    public BigGroup deleteGroup(Id id);
    public BigGroup getGroup(Id id);
    public List<BigGroup> listGroups(List<Id> ids);
    public List<BigGroup> listAllGroups(long limit);

    public Id createTarget(BigTarget target);
    public Id updateTarget(BigTarget target);
    public BigTarget deleteTarget(Id id);
    public BigTarget getTarget(Id id);
    public List<BigTarget> listTargets(List<Id> ids);
    public List<BigTarget> listAllTargets(long limit);

    public Id createBelong(BigBelong belong);
    public Id updateBelong(BigBelong belong);
    public BigBelong deleteBelong(Id id);
    public BigBelong getBelong(Id id);
    public List<BigBelong> listBelong(List<Id> ids);
    public List<BigBelong> listAllBelong(long limit);
    public List<BigBelong> listBelongByUser(Id user, long limit);
    public List<BigBelong> listBelongByGroup(Id group, long limit);

    public Id createAccess(BigAccess access);
    public Id updateAccess(BigAccess access);
    public BigAccess deleteAccess(Id id);
    public BigAccess getAccess(Id id);
    public List<BigAccess> listAccess(List<Id> ids);
    public List<BigAccess> listAllAccess(long limit);
    public List<BigAccess> listAccessByGroup(Id group, long limit);
    public List<BigAccess> listAccessByTarget(Id target, long limit);

    public Id createProject(BigProject project);
    public BigProject deleteProject(Id id);
    public Id updateProject(BigProject project);
    public Id projectAddGraphs(Id id, Set<String> graphs);
    public Id projectRemoveGraphs(Id id, Set<String> graphs);
    public BigProject getProject(Id id);
    public List<BigProject> listAllProject(long limit);

    public BigUser matchUser(String name, String password);
    public RolePermission rolePermission(AuthElement element);

    public String loginUser(String username, String password)
                            throws AuthenticationException;
    public void logoutUser(String token);

    public UserWithRole validateUser(String username, String password);
    public UserWithRole validateUser(String token);
}
