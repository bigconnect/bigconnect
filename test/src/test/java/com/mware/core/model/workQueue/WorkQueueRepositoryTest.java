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
package com.mware.core.model.workQueue;

import com.mware.core.config.Configuration;
import com.mware.core.ingest.dataworker.DataWorkerMessage;
import com.mware.core.model.clientapi.dto.ClientApiWorkspace;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.types.BcPropertyUpdate;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workspace.Workspace;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.user.User;
import com.mware.ge.*;
import com.mware.ge.inmemory.InMemoryGraph;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WorkQueueRepositoryTest {
    private TestWorkQueueRepository workQueueRepository;
    private TestWebQueueRepository webQueueRepository;
    private Graph graph;
    private Authorizations authorizations;

    @Mock
    private Configuration configuration;

    @Mock
    private UserRepository userRepository;

    @Mock
    private AuthorizationRepository authorizationRepository;

    @Mock
    private WorkspaceRepository workspaceRepository;

    @Mock
    private User mockUser1;

    @Mock
    private User mockUser2;

    @Mock
    private Workspace workspace;

    @Before
    public void before() {
        graph = InMemoryGraph.create();
        authorizations = graph.createAuthorizations();
        workQueueRepository = new TestWorkQueueRepository(
                graph,
                configuration
        );
        webQueueRepository = new TestWebQueueRepository();
        webQueueRepository.setAuthorizationRepository(authorizationRepository);
        webQueueRepository.setUserRepository(userRepository);
        webQueueRepository.setWorkspaceRepository(workspaceRepository);
    }

    @Test
    public void testPushWorkspaceChangeSameUser() {
        ClientApiWorkspace workspace = new ClientApiWorkspace();
        List<ClientApiWorkspace.User> previousUsers = new ArrayList<>();
        ClientApiWorkspace.User previousUser = new ClientApiWorkspace.User();
        previousUser.setUserId("user123");
        previousUsers.add(previousUser);
        String changedByUserId = "user123";
        String changedBySourceGuid = "123-123-1234";

        webQueueRepository.broadcastWorkspace(workspace, previousUsers, changedByUserId, changedBySourceGuid);

        assertEquals(1, webQueueRepository.broadcastJsonValues.size());
        JSONObject json = webQueueRepository.broadcastJsonValues.get(0);
        assertEquals("workspaceChange", json.getString("type"));
        assertEquals("user123", json.getString("modifiedBy"));
        assertEquals(new JSONObject("{\"users\":[\"user123\"]}").toString(), json.getJSONObject("permissions").toString());
        assertEquals(
                new JSONObject("{\"editable\":false,\"users\":[],\"commentable\":false,\"sharedToUser\":false}").toString(),
                json.getJSONObject("data").toString()
        );
        assertEquals("123-123-1234", json.getString("sourceGuid"));
    }

    @Test
    public void testPushWorkspaceChangeDifferentUser() {
        ClientApiWorkspace clientApiWorkspace = new ClientApiWorkspace();
        clientApiWorkspace.setWorkspaceId("ws1");
        List<ClientApiWorkspace.User> previousUsers = new ArrayList<>();
        ClientApiWorkspace.User previousUser = new ClientApiWorkspace.User();
        previousUser.setUserId("mockUser1");
        previousUsers.add(previousUser);
        String changedByUserId = "mockUser2";
        String changedBySourceGuid = "123-123-1234";

        Authorizations mockUser1Auths = graph.createAuthorizations("mockUser1Auths");

        when(userRepository.findById(changedByUserId)).thenReturn(mockUser2);
        when(workspaceRepository.findById(eq("ws1"), eq(mockUser2))).thenReturn(workspace);
        when(userRepository.findById(eq("mockUser1"))).thenReturn(mockUser1);
        when(authorizationRepository.getGraphAuthorizations(eq(mockUser1), eq("ws1"))).thenReturn(mockUser1Auths);
        when(workspaceRepository.toClientApi(eq(workspace), eq(mockUser1), any())).thenReturn(clientApiWorkspace);

        webQueueRepository.broadcastWorkspace(clientApiWorkspace, previousUsers, changedByUserId, changedBySourceGuid);

        assertEquals(1, webQueueRepository.broadcastJsonValues.size());
        JSONObject json = webQueueRepository.broadcastJsonValues.get(0);
        assertEquals("workspaceChange", json.getString("type"));
        assertEquals("mockUser2", json.getString("modifiedBy"));
        assertEquals(new JSONObject("{\"users\":[\"mockUser1\"]}").toString(), json.getJSONObject("permissions").toString());
        assertEquals(
                new JSONObject("{\"editable\":false,\"users\":[],\"commentable\":false,\"workspaceId\":\"ws1\",\"sharedToUser\":false}").toString(),
                json.getJSONObject("data").toString()
        );
        assertEquals("123-123-1234", json.getString("sourceGuid"));
    }

    @Test
    public void testPushGraphPropertyQueue() {
        Visibility visibility = new Visibility("");
        VertexBuilder m = graph.prepareVertex("v1", visibility, SchemaConstants.CONCEPT_TYPE_THING);
        BcSchema.COMMENT.addPropertyValue(m, "k1", "comment1", visibility);
        BcSchema.COMMENT.addPropertyValue(m, "k2", "comment2", visibility);
        BcSchema.COMMENT.addPropertyValue(m, "k3", "comment3", visibility);
        Vertex element = m.save(authorizations);

        List<BcPropertyUpdate> properties = new ArrayList<>();
        properties.add(new BcPropertyUpdate(BcSchema.COMMENT, "k1"));
        properties.add(new BcPropertyUpdate(BcSchema.COMMENT, "k2"));
        properties.add(new BcPropertyUpdate(BcSchema.COMMENT, "k3"));

        webQueueRepository.broadcastPropertiesChange(element, properties, null, Priority.HIGH);
        workQueueRepository.pushGraphPropertyQueue(element, properties, null, null, Priority.HIGH);

        assertEquals(1, workQueueRepository.getWorkQueue(workQueueRepository.getQueueName()).size());
        DataWorkerMessage message = DataWorkerMessage.create(workQueueRepository.getWorkQueue(workQueueRepository.getQueueName()).get(0));
        assertEquals(3, message.getProperties().length);
    }
}
