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
package com.mware.core.model.user.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mware.core.cmdline.CommandLineTool;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.UserStatus;
import com.mware.core.model.properties.UserSchema;
import com.mware.core.model.user.cli.args.*;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.values.storable.ByteArray;

import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.mware.ge.util.IterableUtils.toList;

@Parameters(commandDescription = "User administration")
public class UserAdmin extends CommandLineTool {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(UserAdmin.class, "cli-userAdmin");
    public static final String ACTION_CREATE = "create";
    public static final String ACTION_LIST = "list";
    public static final String ACTION_ACTIVE = "active";
    public static final String ACTION_EXPORT_PASSWORDS = "export-passwords";
    public static final String ACTION_UPDATE_PASSWORD = "update-password";
    public static final String ACTION_DELETE = "delete";
    public static final String ACTION_SET_DISPLAYNAME_AND_OR_EMAIL = "set-displayname-and-or-email";
    private Args args;
    private List<String> actions = new ArrayList<>();
    private String userAdminAction;
    private Collection<String> authorizationRepositoryActions;
    private Collection<String> privilegeRepositoryActions;

    public static void main(String[] args) throws Exception {
        CommandLineTool.main(new UserAdmin(), args);
    }

    @Override
    protected JCommander parseArguments(String[] args) {
        actions.add(ACTION_CREATE);
        actions.add(ACTION_LIST);
        actions.add(ACTION_ACTIVE);
        actions.add(ACTION_EXPORT_PASSWORDS);
        actions.add(ACTION_UPDATE_PASSWORD);
        actions.add(ACTION_DELETE);
        actions.add(ACTION_SET_DISPLAYNAME_AND_OR_EMAIL);

        // need to initialize framework early to get repositories
        initializeFramework();

        if (getAuthorizationRepositoryCliService() != null) {
            authorizationRepositoryActions = getAuthorizationRepositoryCliService().getActions(this);
            actions.addAll(authorizationRepositoryActions);
        }
        if (getPrivilegeRepositoryCliService() != null) {
            privilegeRepositoryActions = getPrivilegeRepositoryCliService().getActions(this);
            actions.addAll(privilegeRepositoryActions);
        }

        if (args.length == 0 || !actions.contains(args[0])) {
            System.err.println("Action must be one of: " + Joiner.on(" | ").join(actions));
            return null;
        }
        userAdminAction = args[0];
        if (userAdminAction == null) {
            throw new BcException("Could not parse UserAdminAction");
        }

        this.args = getArgumentsObject();

        JCommander j = new JCommander(this.args, Arrays.copyOfRange(args, 1, args.length));
        if (this.args.help) {
            this.printHelp(j);
            return null;
        } else {
            this.args.validate(j);
        }
        if (getAuthorizationRepositoryCliService() != null) {
            getAuthorizationRepositoryCliService().validateArguments(this, userAdminAction, this.args);
        }
        if (getPrivilegeRepository() instanceof PrivilegeRepositoryWithCliSupport) {
            getPrivilegeRepositoryCliService().validateArguments(this, userAdminAction, this.args);
        }
        return j;
    }

    @Override
    protected void printHelp(JCommander j) {
        super.printHelp(j);
        if (getAuthorizationRepositoryCliService() != null) {
            getAuthorizationRepositoryCliService().printHelp(this, userAdminAction);
        }
        if (getPrivilegeRepository() instanceof PrivilegeRepositoryWithCliSupport) {
            getPrivilegeRepositoryCliService().printHelp(this, userAdminAction);
        }
    }

    private Args getArgumentsObject() {
        switch (userAdminAction) {
            case ACTION_CREATE:
                return new CreateUserArgs();
            case ACTION_LIST:
                return new ListUsersArgs();
            case ACTION_ACTIVE:
                return new ListActiveUsersArgs();
            case ACTION_EXPORT_PASSWORDS:
                return new ExportPasswordsArgs();
            case ACTION_UPDATE_PASSWORD:
                return new UpdatePasswordArgs();
            case ACTION_DELETE:
                return new DeleteUserArgs();
            case ACTION_SET_DISPLAYNAME_AND_OR_EMAIL:
                return new SetDisplayNameEmailArgs();
            default:
                if (getAuthorizationRepositoryCliService() != null
                        && authorizationRepositoryActions.contains(userAdminAction)) {
                    return getAuthorizationRepositoryCliService().createArguments(this, userAdminAction);
                }
                if (getPrivilegeRepositoryCliService() != null
                        && privilegeRepositoryActions.contains(userAdminAction)) {
                    return getPrivilegeRepositoryCliService().createArguments(this, userAdminAction);
                }
                break;
        }
        throw new BcException("Unhandled userAdminAction: " + userAdminAction);
    }

    @Override
    protected int run() throws Exception {
        LOGGER.info("running %s", userAdminAction);
        try {
            switch (userAdminAction) {
                case ACTION_CREATE:
                    return create((CreateUserArgs) this.args);
                case ACTION_LIST:
                    return list((ListUsersArgs) this.args);
                case ACTION_ACTIVE:
                    return active((ListActiveUsersArgs) this.args);
                case ACTION_EXPORT_PASSWORDS:
                    return exportPasswords((ExportPasswordsArgs) this.args);
                case ACTION_UPDATE_PASSWORD:
                    return updatePassword((UpdatePasswordArgs) this.args);
                case ACTION_DELETE:
                    return delete((DeleteUserArgs) this.args);
                case ACTION_SET_DISPLAYNAME_AND_OR_EMAIL:
                    return setDisplayNameAndOrEmail((SetDisplayNameEmailArgs) this.args);
                default:
                    if (getAuthorizationRepositoryCliService() != null
                            && authorizationRepositoryActions.contains(userAdminAction)) {
                        return getAuthorizationRepositoryCliService().run(this, userAdminAction, this.args, getUser());
                    }
                    if (getPrivilegeRepositoryCliService() != null
                            && privilegeRepositoryActions.contains(userAdminAction)) {
                        return getPrivilegeRepositoryCliService().run(this, userAdminAction, this.args, getUser());
                    }
                    break;
            }
        } catch (UserNotFoundException ex) {
            System.err.println(ex.getMessage());
            return 2;
        }
        throw new BcException("Unhandled userAdminAction: " + userAdminAction);
    }

    private int exportPasswords(ExportPasswordsArgs args) {
        List<User> sortedUsers = loadUsers().stream()
                .sorted((u1, u2) -> u1.getUsername().compareTo(u2.getUsername()))
                .collect(Collectors.toList());

        if (!sortedUsers.isEmpty()) {
            int maxUsernameWidth = sortedUsers.stream()
                    .map(User::getUsername)
                    .map(String::length)
                    .max(Integer::compareTo)
                    .orElseGet(() -> 0);
            String format = String.format("%%%ds %%s%%n", -1 * maxUsernameWidth);
            for (User user : sortedUsers) {
                byte[] bPasswordSalt = ((ByteArray) user.getProperty(UserSchema.PASSWORD_SALT.getPropertyName())).asObjectCopy();
                byte[] bPasswordHash = ((ByteArray) user.getProperty(UserSchema.PASSWORD_HASH.getPropertyName())).asObjectCopy();
                String passwordSalt = Base64.getEncoder().encodeToString(bPasswordSalt);
                String passwordHash = Base64.getEncoder().encodeToString(bPasswordHash);
                System.out.printf(
                        format,
                        user.getUsername(),
                        passwordSalt + ":" + passwordHash
                );
            }
        } else {
            System.out.println("No users");
        }

        return 0;
    }

    private int create(CreateUserArgs args) {
        getUserRepository().findOrAddUser(
                args.userName,
                args.userName,
                null,
                args.password
        );

        User user = getUserRepository().findByUsername(args.userName);

        if (args.displayName != null) {
            getUserRepository().setDisplayName(user, args.displayName);
        }
        if (args.email != null) {
            getUserRepository().setEmailAddress(user, args.email);
        }

        if (getAuthorizationRepositoryCliService() != null) {
            getAuthorizationRepositoryCliService().onCreateUser(this, args, user, getUser());
        }
        if (getPrivilegeRepositoryCliService() != null) {
            getPrivilegeRepositoryCliService().onCreateUser(this, args, user, getUser());
        }

        printUser(getUserRepository().findById(user.getUserId()));
        return 0;
    }

    private int list(ListUsersArgs args) {
        List<User> sortedUsers = loadUsers().stream().sorted((u1, u2) -> {
            ZonedDateTime d1 = u1.getCreateDate();
            ZonedDateTime d2 = u2.getCreateDate();
            return d1 == d2 ? 0 : d1.compareTo(d2);
        }).collect(Collectors.toList());

        if (args.asTable) {
            printUsers(sortedUsers);
        } else {
            sortedUsers.forEach(this::printUser);
        }
        return 0;
    }

    private int active(ListActiveUsersArgs args) {
        List<User> activeUsers = loadUsers(UserStatus.ACTIVE);
        System.out.println(activeUsers.size() + " " + UserStatus.ACTIVE + " user" + (activeUsers.size() == 1 ? "" : "s"));
        printUsers(activeUsers);
        return 0;
    }

    private int updatePassword(UpdatePasswordArgs args) {
        User user = findUser(args);
        if (!Strings.isNullOrEmpty(args.password)) {
            getUserRepository().setPassword(user, args.password);
        } else if (!Strings.isNullOrEmpty(args.passwordSaltAndHash)) {
            String[] saltAndHashStrings = args.passwordSaltAndHash.split(":", -1);
            byte[] salt = Base64.getDecoder().decode(saltAndHashStrings[0]);
            byte[] passwordHash = Base64.getDecoder().decode(saltAndHashStrings[1]);
            getUserRepository().setPassword(user, salt, passwordHash);
        }
        printUser(user);
        return 0;
    }

    private int delete(DeleteUserArgs args) {
        User user = findUser(args);
        getUserRepository().delete(user);
        System.out.println("Deleted user " + user.getUserId());
        return 0;
    }

    private int setDisplayNameAndOrEmail(SetDisplayNameEmailArgs args) {
        if (args.displayName == null && args.email == null) {
            System.out.println("no display name or e-mail address provided");
            return -2;
        }

        User user = findUser(args);

        if (args.displayName != null) {
            getUserRepository().setDisplayName(user, args.displayName);
        }
        if (args.email != null) {
            getUserRepository().setEmailAddress(user, args.email);
        }

        printUser(getUserRepository().findById(user.getUserId()));
        return 0;
    }

    public User findUser(FindUserArgs findUserArgs) {
        User user = null;
        if (findUserArgs.userName != null) {
            user = getUserRepository().findByUsername(findUserArgs.userName);
        } else if (findUserArgs.userId != null) {
            user = getUserRepository().findById(findUserArgs.userId);
        }

        if (user == null) {
            throw new UserNotFoundException(findUserArgs);
        }

        return user;
    }

    public void printUser(User user) {
        String formatString = "%30s: %s";
        System.out.println(String.format(formatString, "ID", user.getUserId()));
        System.out.println(String.format(formatString, "Username", user.getUsername()));
        System.out.println(String.format(formatString, "E-Mail Address", valueOrBlank(user.getEmailAddress())));
        System.out.println(String.format(formatString, "Display Name", user.getDisplayName()));
        System.out.println(String.format(formatString, "Create Date", valueOrBlank(user.getCreateDate())));
        System.out.println(String.format(formatString, "Current Login Date", valueOrBlank(user.getCurrentLoginDate())));
        System.out.println(String.format(formatString, "Current Login Remote Addr", valueOrBlank(user.getCurrentLoginRemoteAddr())));
        System.out.println(String.format(formatString, "Previous Login Date", valueOrBlank(user.getPreviousLoginDate())));
        System.out.println(String.format(formatString, "Previous Login Remote Addr", valueOrBlank(user.getPreviousLoginRemoteAddr())));
        System.out.println(String.format(formatString, "Login Count", user.getLoginCount()));
        if (getAuthorizationRepositoryCliService() != null) {
            getAuthorizationRepositoryCliService().onPrintUser(this, this.args, formatString, user);
        }
        if (getPrivilegeRepositoryCliService() != null) {
            getPrivilegeRepositoryCliService().onPrintUser(this, this.args, formatString, user);
        }
        System.out.println("");
    }

    private void printUsers(Iterable<User> users) {
        if (users != null) {
            int maxCreateDateWidth = 1;
            int maxIdWidth = 1;
            int maxUsernameWidth = 1;
            int maxEmailAddressWidth = 1;
            int maxDisplayNameWidth = 1;
            int maxLoginCountWidth = 1;
            for (User user : users) {
                maxCreateDateWidth = maxWidth(user.getCreateDate(), maxCreateDateWidth);
                maxIdWidth = maxWidth(user.getUserId(), maxIdWidth);
                maxUsernameWidth = maxWidth(user.getUsername(), maxUsernameWidth);
                maxEmailAddressWidth = maxWidth(user.getEmailAddress(), maxEmailAddressWidth);
                maxDisplayNameWidth = maxWidth(user.getDisplayName(), maxDisplayNameWidth);
                maxLoginCountWidth = maxWidth(Integer.toString(user.getLoginCount()), maxLoginCountWidth);
            }
            String format = String.format(
                    "%%%ds %%%ds %%%ds %%%ds %%%ds %%%dd %%n",
                    -1 * maxCreateDateWidth,
                    -1 * maxIdWidth,
                    -1 * maxUsernameWidth,
                    -1 * maxEmailAddressWidth,
                    -1 * maxDisplayNameWidth,
                    maxLoginCountWidth
            );
            for (User user : users) {
                System.out.printf(
                        format,
                        valueOrBlank(user.getCreateDate()),
                        user.getUserId(),
                        user.getUsername(),
                        valueOrBlank(user.getEmailAddress()),
                        user.getDisplayName(),
                        user.getLoginCount()
                );
            }
        } else {
            System.out.println("No users");
        }
    }

    private String valueOrBlank(Object o) {
        if (o == null) {
            return "";
        } else if (o instanceof Date) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
            return sdf.format(o);
        } else {
            return o.toString();
        }
    }

    private int maxWidth(Object o, int max) {
        int width = valueOrBlank(o).length();
        return width > max ? width : max;
    }

    private List<User> loadUsers() {
        return loadUsers(null);
    }

    private List<User> loadUsers(UserStatus filter) {
        List<User> allUsers = new ArrayList<>();

        int limit = 100;
        for (int skip = 0; ; skip += limit) {
            Iterable<User> usersIterable = (filter == null) ?
                    getUserRepository().find(skip, limit) :
                    getUserRepository().findByStatus(skip, limit, filter);

            List<User> userPage = toList(usersIterable);
            if (userPage.size() == 0) {
                break;
            }
            allUsers.addAll(userPage);
        }
        return allUsers;
    }

    private PrivilegeRepositoryCliService getPrivilegeRepositoryCliService() {
        if (getPrivilegeRepository() instanceof PrivilegeRepositoryWithCliSupport) {
            return ((PrivilegeRepositoryWithCliSupport) getPrivilegeRepository()).getCliService();
        }
        return null;
    }

    private AuthorizationRepositoryCliService getAuthorizationRepositoryCliService() {
        if (getAuthorizationRepository() instanceof AuthorizationRepositoryWithCliSupport) {
            return ((AuthorizationRepositoryWithCliSupport) getAuthorizationRepository()).getCliService();
        }
        return null;
    }
}
