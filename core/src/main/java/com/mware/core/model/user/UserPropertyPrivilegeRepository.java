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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.role.Role;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Values;
import org.json.JSONObject;
import com.mware.ge.TextIndexHint;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configurable;
import com.mware.core.config.Configuration;
import com.mware.core.model.notification.ExpirationAge;
import com.mware.core.model.notification.UserNotification;
import com.mware.core.model.notification.UserNotificationRepository;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.SchemaPropertyDefinition;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.user.cli.PrivilegeRepositoryCliService;
import com.mware.core.model.user.cli.PrivilegeRepositoryWithCliSupport;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.clientapi.dto.PropertyType;

import java.util.*;
import java.util.stream.Collectors;

import static com.mware.core.model.schema.SchemaRepository.PUBLIC;

public class UserPropertyPrivilegeRepository extends PrivilegeRepositoryBase implements PrivilegeRepositoryWithCliSupport {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(UserPropertyPrivilegeRepository.class);
    public static final String PRIVILEGES_PROPERTY_NAME = "userPrivileges";
    public static final String CONFIGURATION_PREFIX = UserPropertyPrivilegeRepository.class.getName();
    private final ImmutableSet<String> defaultPrivileges;
    private final Configuration configuration;
    private final UserNotificationRepository userNotificationRepository;
    private final WebQueueRepository webQueueRepository;
    private final AuthorizationRepository authorizationRepository;
    private Collection<UserListener> userListeners;

    private static class Settings {
        @Configurable()
        public String defaultPrivileges;
    }

    @Inject
    public UserPropertyPrivilegeRepository(
            SchemaRepository schemaRepository,
            Configuration configuration,
            UserNotificationRepository userNotificationRepository,
            WebQueueRepository webQueueRepository,
            AuthorizationRepository authorizationRepository
    ) {
        super(configuration);
        this.configuration = configuration;
        this.userNotificationRepository = userNotificationRepository;
        this.webQueueRepository = webQueueRepository;
        this.authorizationRepository = authorizationRepository;

        definePrivilegesProperty(schemaRepository);

        Settings settings = new Settings();
        configuration.setConfigurables(settings, CONFIGURATION_PREFIX);
        this.defaultPrivileges = ImmutableSet.copyOf(Privilege.stringToPrivileges(settings.defaultPrivileges));
    }

    private void definePrivilegesProperty(SchemaRepository schemaRepository) {
        List<Concept> concepts = new ArrayList<>();
        concepts.add(schemaRepository.getConceptByName(UserRepository.USER_CONCEPT_NAME, PUBLIC));
        SchemaPropertyDefinition propertyDefinition = new SchemaPropertyDefinition(
                concepts,
                PRIVILEGES_PROPERTY_NAME,
                "Privileges",
                PropertyType.STRING
        );
        propertyDefinition.setUserVisible(false);
        propertyDefinition.setTextIndexHints(TextIndexHint.NONE);
        schemaRepository.getOrCreateProperty(propertyDefinition, new SystemUser(), PUBLIC);
    }

    @Override
    public void updateUser(User user, AuthorizationContext authorizationContext) {
    }

    @Override
    public Set<String> getPrivileges(User user) {
        if (user instanceof SystemUser) {
            return Sets.newHashSet();
        }

        if (user == null) {
            return new HashSet<>(defaultPrivileges);
        }

        TextValue privileges = (TextValue) user.getProperty(PRIVILEGES_PROPERTY_NAME);
        Set<String> result = new HashSet<>(Privilege.stringToPrivileges(privileges == null ? null : privileges.stringValue()));
        Set<Role> roles = authorizationRepository.getRoles(user);
        for(Role role : roles) {
            if(role.getPrivileges() != null) {
                result.addAll(role.getPrivileges().stream()
                        .map(p -> p.getName())
                        .collect(Collectors.toSet())
                );
            }
        }

        if(result.isEmpty()) {
            result = new HashSet<>(defaultPrivileges);
        }
        return result;
    }

    public void setPrivileges(User user, Set<String> privileges, User authUser) {
        if (!privileges.equals(getPrivileges(user))) {
            String privilegesString = Privilege.toString(privileges);
            LOGGER.info(
                    "Setting privileges to '%s' on user '%s' by '%s'",
                    privilegesString,
                    user.getUsername(),
                    authUser.getUsername()
            );
            getUserRepository().setPropertyOnUser(user, PRIVILEGES_PROPERTY_NAME, Values.stringValue(privilegesString));
            sendNotificationToUserAboutPrivilegeChange(user, privileges, authUser);
            webQueueRepository.pushUserAccessChange(user);
            fireUserPrivilegesUpdatedEvent(user, privileges);
        }
    }

    private void sendNotificationToUserAboutPrivilegeChange(User user, Set<String> privileges, User authUser) {
        String title = "Privileges Changed";
        String message = "New Privileges: " + Joiner.on(", ").join(privileges);
        String actionEvent = null;
        JSONObject actionPayload = null;
        ExpirationAge expirationAge = null;
        UserNotification userNotification = userNotificationRepository.createNotification(
                user.getUserId(),
                title,
                message,
                actionEvent,
                actionPayload,
                expirationAge,
                authUser
        );
        webQueueRepository.pushUserNotification(userNotification);
    }

    private void fireUserPrivilegesUpdatedEvent(User user, Set<String> privileges) {
        for (UserListener userListener : getUserListeners()) {
            userListener.userPrivilegesUpdated(user, privileges);
        }
    }

    private Collection<UserListener> getUserListeners() {
        if (userListeners == null) {
            userListeners = InjectHelper.getInjectedServices(UserListener.class, configuration);
        }
        return userListeners;
    }

    @Override
    public PrivilegeRepositoryCliService getCliService() {
        return new UserPropertyPrivilegeRepositoryCliService(this);
    }

    public ImmutableSet<String> getDefaultPrivileges() {
        return defaultPrivileges;
    }
}
