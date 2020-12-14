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

import com.google.common.annotations.VisibleForTesting;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.user.User;
import com.mware.core.model.clientapi.dto.Privilege;

import java.util.HashSet;
import java.util.Set;

public abstract class PrivilegeRepositoryBase implements PrivilegeRepository {
    private final Iterable<PrivilegesProvider> privilegesProviders;
    private UserRepository userRepository;

    protected PrivilegeRepositoryBase(Configuration configuration) {
        this.privilegesProviders = getPrivilegesProviders(configuration);
    }

    protected Iterable<PrivilegesProvider> getPrivilegesProviders(Configuration configuration) {
        return InjectHelper.getInjectedServices(PrivilegesProvider.class, configuration);
    }

    public boolean hasPrivilege(User user, String privilege) {
        Set<String> privileges = getPrivileges(user);
        return PrivilegeRepository.hasPrivilege(privileges, privilege);
    }

    public boolean hasAllPrivileges(User user, Set<String> requiredPrivileges) {
        return Privilege.hasAll(getPrivileges(user), requiredPrivileges);
    }

    // Need to late bind since UserRepository injects AuthorizationRepository in constructor
    protected UserRepository getUserRepository() {
        if (userRepository == null) {
            userRepository = InjectHelper.getInstance(UserRepository.class);
        }
        return userRepository;
    }

    @VisibleForTesting
    public void setUserRepository(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public abstract void updateUser(User user, AuthorizationContext authorizationContext);

    @Override
    public abstract Set<String> getPrivileges(User user);

    @Override
    public Set<Privilege> getAllPrivileges() {
        Set<Privilege> privileges = new HashSet<>();
        for (PrivilegesProvider privilegesProvider : privilegesProviders) {
            for (Privilege privilege : privilegesProvider.getPrivileges()) {
                privileges.add(privilege);
            }
        }
        return privileges;
    }

    protected Privilege findPrivilegeByName(String privilegeName) {
        for (Privilege p : getAllPrivileges()) {
            if (p.getName().equals(privilegeName)) {
                return p;
            }
        }
        return null;
    }
}
