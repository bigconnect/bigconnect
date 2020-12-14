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

import com.beust.jcommander.Parameter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.mware.core.model.user.cli.UserAdmin;
import com.mware.core.model.user.cli.args.Args;
import org.apache.commons.lang.StringUtils;
import com.mware.core.exception.BcException;
import com.mware.core.model.user.cli.PrivilegeRepositoryCliService;
import com.mware.core.model.user.cli.args.CreateUserArgs;
import com.mware.core.model.user.cli.args.FindUserArgs;
import com.mware.core.user.User;
import com.mware.core.model.clientapi.dto.Privilege;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class UserPropertyPrivilegeRepositoryCliService implements PrivilegeRepositoryCliService {
    private static final String CLI_PARAMETER_PRIVILEGES = "privileges";
    private static final String ACTION_SET_PRIVILEGES = "set-privileges";
    private static final char SEPARATOR_CHAR = ',';

    private final UserPropertyPrivilegeRepository privilegeRepository;

    public UserPropertyPrivilegeRepositoryCliService(UserPropertyPrivilegeRepository privilegeRepository) {
        this.privilegeRepository = privilegeRepository;
    }

    @Override
    public void onCreateUser(UserAdmin userAdmin, CreateUserArgs createUserArgs, User user, User authUser) {
        String privilegesString = createUserArgs.privilegeRepositoryArguments.get(CLI_PARAMETER_PRIVILEGES);
        ImmutableSet<String> newPrivileges;
        if (privilegesString == null) {
            newPrivileges = privilegeRepository.getDefaultPrivileges();
        } else {
            String[] privileges = privilegesString.split(",");
            newPrivileges = ImmutableSet.copyOf(privileges);
        }
        privilegeRepository.setPrivileges(user, newPrivileges, authUser);
    }

    @Override
    public void onPrintUser(UserAdmin userAdmin, Args args, String formatString, User user) {
        Set<String> privileges = privilegeRepository.getPrivileges(user);
        String privilegesString = Joiner.on(",").join(privileges);
        System.out.println(String.format(formatString, "Privileges", privilegesString));
    }

    @Override
    public Collection<String> getActions(UserAdmin userAdmin) {
        return ImmutableList.of(ACTION_SET_PRIVILEGES);
    }

    @Override
    public Args createArguments(UserAdmin userAdmin, String action) {
        switch (action) {
            case ACTION_SET_PRIVILEGES:
                return new SetPrivilegesCliArguments();
        }
        return null;
    }

    @Override
    public int run(UserAdmin userAdmin, String action, Args args, User authUser) {
        switch (action) {
            case ACTION_SET_PRIVILEGES:
                return cliRunSetPrivileges(userAdmin, (SetPrivilegesCliArguments) args, authUser);
        }
        throw new BcException("Unhandled cli action " + action);
    }

    private int cliRunSetPrivileges(UserAdmin userAdmin, SetPrivilegesCliArguments args, User authUser) {
        Set<String> privileges = new HashSet<>();
        if (args.privileges != null && args.privileges.length() > 0) {
            privileges.addAll(Arrays.asList(StringUtils.split(args.privileges, SEPARATOR_CHAR)));
        }

        User user = userAdmin.findUser(args);
        privilegeRepository.setPrivileges(user, privileges, authUser);
        userAdmin.printUser(user);
        return 0;
    }

    @Override
    public void validateArguments(UserAdmin userAdmin, String action, Args args) {
        switch (action) {
            case UserAdmin.ACTION_CREATE:
                validateCreateUserArguments((CreateUserArgs) args);
                break;
            case ACTION_SET_PRIVILEGES:
                validateSetPrivilegesArguments((SetPrivilegesCliArguments) args);
                break;
        }
    }

    @Override
    public void printHelp(UserAdmin userAdmin, String action) {
        switch (action) {
            case UserAdmin.ACTION_CREATE:
            case ACTION_SET_PRIVILEGES:
                System.out.println("  Privileges:");
                System.out.println("    -P" + CLI_PARAMETER_PRIVILEGES + "=<privileges>");
                System.out.println("       Comma separated list of privileges");
                System.out.println();
                break;
        }
    }

    private void validateSetPrivilegesArguments(SetPrivilegesCliArguments args) {
        validatePrivileges(StringUtils.split(args.privileges, SEPARATOR_CHAR));
    }

    private void validateCreateUserArguments(CreateUserArgs args) {
        int s = args.privilegeRepositoryArguments.size();
        if (s == 0) {
            return;
        }
        String privsString = args.privilegeRepositoryArguments.get(CLI_PARAMETER_PRIVILEGES);
        if ((s == 1 && privsString == null) || s != 1) {
            throw new BcException(this.getClass().getName() + " expects no parameters or '" + CLI_PARAMETER_PRIVILEGES + "'");
        }
        validatePrivileges(StringUtils.split(privsString, SEPARATOR_CHAR));
    }

    private void validatePrivileges(String[] privileges) {
        for (String privilege : privileges) {
            if (privilegeRepository.findPrivilegeByName(privilege) == null) {
                throw new BcException(
                        "Unexpected privilege \"" + privilege + "\". Expected one of ["
                                + Privilege.toStringPrivileges(privilegeRepository.getAllPrivileges()) + "]");
            }
        }
    }

    public static class SetPrivilegesCliArguments extends FindUserArgs {
        @Parameter(names = {"--privileges", "-p"}, arity = 1, required = true, description = "Comma separated list of privileges to set, or none")
        public String privileges;
    }
}
