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

import com.google.common.collect.Sets;
import com.mware.core.model.role.Role;
import com.mware.core.model.workQueue.TestWebQueueRepository;
import com.mware.core.user.User;
import com.mware.core.GraphTestBase;
import com.mware.core.util.JSONUtil;
import com.mware.ge.base.TestGraphFactory;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class GeAuthorizationRepositoryTest extends GraphTestBase {
    @Mock
    private User user1;

    private User user2;

    @Before
    public void before() {
        user2 = getUserRepository().findOrAddUser("user2", "User 2", "user2@example.com", "password");
        getAuthorizationRepository().setRolesForUser(user2, Collections.emptySet(), user1);
    }

    @Override
    protected TestGraphFactory graphFactory() {
        return null;
    }

    @Test
    public void testGetAuthorizationsForExisting() {
//        String[] authorizationsArray = {"userAuthorization1", "userAuthorization2", "userRepositoryAuthorization1", "userRepositoryAuthorization2"};
//        when(user1.getProperty(eq(GeAuthorizationRepository.AUTHORIZATIONS_PROPERTY_IRI)))
//                .thenReturn("userAuthorization1,userAuthorization2");
//
//        Set<String> privileges = getAuthorizationRepository().getAuthorizations(user1);
//        assertEquals(Sets.newHashSet(authorizationsArray), privileges);
    }

    @Test
    public void testAddRole() {
        String[] authorizationsArray = {"newauth"};
        String roleName = "newAuth";
        Role r = getAuthorizationRepository().addRole(roleName, "", true, Collections.emptySet());

        getAuthorizationRepository().addRoleToUser(user2, r, user1);
        Set<Role> authorizations = getAuthorizationRepository().getRoles(user2);

        JSONObject broadcast = ((TestWebQueueRepository) getWebQueueRepository()).getLastBroadcastedJson();

        assertNotNull("Should have broadcasted change", broadcast);
        assertEquals("userAccessChange", broadcast.optString("type", null));
        assertEquals(user2.getUserId(), broadcast.getJSONObject("permissions").getJSONArray("users").get(0));

        Set expected = Sets.newHashSet(authorizationsArray);
        Set broadcastedAuths = new HashSet(JSONUtil.toStringList(broadcast.getJSONObject("data").getJSONArray("authorizations")));

        assertEquals(expected, broadcastedAuths);
        assertEquals(Sets.newHashSet(authorizationsArray), authorizations.stream().map(rr -> r.getRoleName()).collect(Collectors.toSet()));
    }

    @Test
    public void testAddAuthorizationShouldTrimWhitespace() {
        String[] authorizationsArray = {"newauth"};
        String authorization = "  newAuth  \n";
        Role r = getAuthorizationRepository().addRole(authorization, "", true, Collections.emptySet());

        getAuthorizationRepository().addRoleToUser(user2, r, user1);
        Set<Role> authorizations = getAuthorizationRepository().getRoles(user2);

        JSONObject broadcast = ((TestWebQueueRepository) getWebQueueRepository()).getLastBroadcastedJson();

        assertNotNull("Should have broadcasted change", broadcast);
        assertEquals("userAccessChange", broadcast.optString("type", null));
        assertEquals(user2.getUserId(), broadcast.getJSONObject("permissions").getJSONArray("users").get(0));


        Set expected = Sets.newHashSet(authorizationsArray);
        Set broadcastedAuths = new HashSet(JSONUtil.toStringList(broadcast.getJSONObject("data").getJSONArray("authorizations")));

        assertEquals(expected, broadcastedAuths);
        assertEquals(expected, authorizations.stream().map(Role::getRoleName).collect(Collectors.toSet()));
    }
}
