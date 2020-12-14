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

import com.mware.core.model.clientapi.dto.ClientApiRole;
import com.mware.core.model.clientapi.dto.ClientApiRoles;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.user.AuthorizationContext;
import com.mware.core.user.User;

import java.util.Set;

public interface AuthorizationRepository {
    String ADMIN_ROLE = "administrator";
    String ROLE_CONCEPT_NAME = "__rl";

    /**
     * Called by UserRepository when a user is authenticated possibly by a web authentication handler
     */
    void updateUser(User user, AuthorizationContext authorizationContext);

    Set<Role> getRoles(User user);
    Set<String> getRoleNames(User user);

    com.mware.ge.Authorizations getGraphAuthorizations(String userName, String... additionalAuthorizations);
    com.mware.ge.Authorizations getGraphAuthorizations(User user, String... additionalAuthorizations);

    Iterable<Role> getAllRoles();

    Role findById(String roleId);

    Role findByName(String roleName);

    Role addRole(String roleName, String description, boolean global, Set<Privilege> privileges);

    void deleteRole(Role role);

    void setRoleName(Role role, String roleName);

    void setDescription(Role role, String description);

    void setGlobal(Role role, boolean global);

    void setPrivileges(Role role, Set<Privilege> privileges);

    void addRoleToUser(User user, String roleName, User authUser);
    void addRoleToUser(User user, Role role, User authUser);
    void addRolesToUser(User user, Set<Role> role, User authUser);

    void removeRoleFromUser(User user, Role role, User authUser);

    void setRolesForUser(User user, Set<Role> newRoles, User authUser);

    ClientApiRoles toClientApi(Iterable<Role> roles);

    ClientApiRole toClientApi(Role role);

    Role getAdministratorRole();
}
