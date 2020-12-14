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

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Sets;
import com.mware.core.config.Configuration;
import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.model.notification.UserNotificationRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.user.User;
import com.mware.ge.Graph;
import com.mware.ge.values.storable.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UserPropertyPrivilegeRepositoryTest {
    private UserPropertyPrivilegeRepository userPropertyPrivilegeRepository;

    @Mock
    private User user;

    @Mock
    private UserRepository userRepository;

    @Mock
    private Graph graph;

    @Mock
    private SchemaRepository schemaRepository;

    @Mock
    private UserNotificationRepository userNotificationRepository;

    @Mock
    private WorkQueueRepository workQueueRepository;

    @Mock
    private WebQueueRepository webQueueRepository;

    @Mock
    private AuthorizationRepository authorizationRepository;

    @Before
    public void before() {
        Map config = new HashMap();
        config.put(
                UserPropertyPrivilegeRepository.CONFIGURATION_PREFIX + ".defaultPrivileges",
                "defaultPrivilege1,defaultPrivilege2"
        );
        Configuration configuration = new HashMapConfigurationLoader(config).createConfiguration();
        userPropertyPrivilegeRepository = new UserPropertyPrivilegeRepository(
                schemaRepository,
                configuration,
                userNotificationRepository,
                webQueueRepository,
                authorizationRepository
        ) {
            @Override
            protected Iterable<PrivilegesProvider> getPrivilegesProviders(Configuration configuration) {
                return Lists.newArrayList();
            }
        };
    }

    @Test
    public void testGetPrivilegesForNewUser() {
        HashSet<String> expected = Sets.newHashSet("defaultPrivilege1", "defaultPrivilege2");

        Set<String> privileges = userPropertyPrivilegeRepository.getPrivileges(user);
        assertEquals(expected, privileges);
    }

    @Test
    public void testGetPrivilegesForExisting() {
        HashSet<String> expected = Sets.newHashSet("userPrivilege1", "userPrivilege2");
        when(user.getProperty(eq(UserPropertyPrivilegeRepository.PRIVILEGES_PROPERTY_NAME)))
                .thenReturn(Values.stringValue("userPrivilege1,userPrivilege2"));

        Set<String> privileges = userPropertyPrivilegeRepository.getPrivileges(user);
        assertEquals(expected, privileges);
    }
}
