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
package com.mware.core.ingest.dataworker;

import com.mware.core.model.workQueue.Priority;
import com.mware.core.util.JSONUtil;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataWorkerMessageTest {
    @Test
    public void testVerticesMessage() {
        String jsonString = "{" +
                "  \"propertyKey\": \"key1\"," +
                "  \"graphVertexId\": [" +
                "    \"v1\"," +
                "    \"v2\"" +
                "  ]," +
                "  \"propertyName\": \"name1\"," +
                "  \"beforeActionTimestamp\": 123456789," +
                "  \"visibilitySource\": \"visibilitySourceValue\"," +
                "  \"priority\": \"HIGH\"," +
                "  \"traceEnabled\": false," +
                "  \"workspaceId\": \"wsTest\"," +
                "  \"status\": \"UPDATE\"" +
                "}";
        DataWorkerMessage message = DataWorkerMessage.create(jsonString.getBytes());
        assertEquals("wsTest", message.getWorkspaceId());
        assertEquals("visibilitySourceValue", message.getVisibilitySource());
        assertEquals("key1", message.getPropertyKey());
        assertEquals("name1", message.getPropertyName());
        assertEquals(ElementOrPropertyStatus.UPDATE, message.getStatus());
        assertEquals(123456789, message.getBeforeActionTimestamp().longValue());
        assertEquals(Priority.HIGH, message.getPriority());
        assertEquals(2, message.getGraphVertexId().length);
        assertEquals("v1", message.getGraphVertexId()[0]);
        assertEquals("v2", message.getGraphVertexId()[1]);
        assertTrue(
                new JSONObject(message.toJsonString()).toString(2),
                JSONUtil.areEqual(new JSONObject(jsonString), new JSONObject(message.toJsonString()))
        );
    }

    @Test
    public void testSingleVertexMessage() {
        String jsonString = "{" +
                "  \"propertyKey\": \"key1\"," +
                "  \"graphVertexId\": \"v1\"," +
                "  \"propertyName\": \"name1\"," +
                "  \"beforeActionTimestamp\": 123456789," +
                "  \"visibilitySource\": \"visibilitySourceValue\"," +
                "  \"priority\": \"HIGH\"," +
                "  \"traceEnabled\": false," +
                "  \"workspaceId\": \"wsTest\"," +
                "  \"status\": \"UPDATE\"" +
                "}";
        DataWorkerMessage message = DataWorkerMessage.create(jsonString.getBytes());
        assertEquals("wsTest", message.getWorkspaceId());
        assertEquals("visibilitySourceValue", message.getVisibilitySource());
        assertEquals("key1", message.getPropertyKey());
        assertEquals("name1", message.getPropertyName());
        assertEquals(ElementOrPropertyStatus.UPDATE, message.getStatus());
        assertEquals(123456789, message.getBeforeActionTimestamp().longValue());
        assertEquals(Priority.HIGH, message.getPriority());
        assertEquals(1, message.getGraphVertexId().length);
        assertEquals("v1", message.getGraphVertexId()[0]);
        assertTrue(
                new JSONObject(message.toJsonString()).toString(2),
                JSONUtil.areEqual(new JSONObject(jsonString), new JSONObject(message.toJsonString()))
        );
    }

    @Test
    public void testMultiplePropertiesMessage() {
        String jsonString = "{" +
                "  \"graphVertexId\": \"v1\"," +
                "  \"visibilitySource\": \"visibilitySourceValue\"," +
                "  \"priority\": \"HIGH\"," +
                "  \"traceEnabled\": false," +
                "  \"properties\": [" +
                "    {" +
                "      \"propertyKey\": \"key1\"," +
                "      \"propertyName\": \"name1\"," +
                "      \"beforeActionTimestamp\": 123456," +
                "      \"status\": \"UPDATE\"" +
                "    }," +
                "    {" +
                "      \"propertyKey\": \"key2\"," +
                "      \"propertyName\": \"name2\"," +
                "      \"beforeActionTimestamp\": 234567," +
                "      \"status\": \"UPDATE\"" +
                "    }" +
                "  ]," +
                "  \"workspaceId\": \"wsTest\"" +
                "}";
        DataWorkerMessage message = DataWorkerMessage.create(jsonString.getBytes());
        assertEquals("wsTest", message.getWorkspaceId());
        assertEquals("visibilitySourceValue", message.getVisibilitySource());
        assertEquals(Priority.HIGH, message.getPriority());
        assertEquals(1, message.getGraphVertexId().length);
        assertEquals("v1", message.getGraphVertexId()[0]);

        assertEquals(2, message.getProperties().length);
        DataWorkerMessage.Property[] properties = message.getProperties();
        DataWorkerMessage.Property property = properties[0];
        assertEquals("key1", property.getPropertyKey());
        assertEquals("name1", property.getPropertyName());
        assertEquals(ElementOrPropertyStatus.UPDATE, property.getStatus());
        assertEquals(123456, property.getBeforeActionTimestamp().longValue());
        property = properties[1];
        assertEquals("key2", property.getPropertyKey());
        assertEquals("name2", property.getPropertyName());
        assertEquals(ElementOrPropertyStatus.UPDATE, property.getStatus());
        assertEquals(234567, property.getBeforeActionTimestamp().longValue());
        assertTrue(
                new JSONObject(message.toJsonString()).toString(2),
                JSONUtil.areEqual(new JSONObject(jsonString), new JSONObject(message.toJsonString()))
        );
    }

    @Test
    public void testEdgesMessage() {
        String jsonString = "{" +
                "  \"propertyKey\": \"key1\"," +
                "  \"graphEdgeId\": [" +
                "    \"e1\"," +
                "    \"e2\"" +
                "  ]," +
                "  \"propertyName\": \"name1\"," +
                "  \"beforeActionTimestamp\": 123456789," +
                "  \"visibilitySource\": \"visibilitySourceValue\"," +
                "  \"priority\": \"HIGH\"," +
                "  \"traceEnabled\": false," +
                "  \"workspaceId\": \"wsTest\"," +
                "  \"status\": \"UPDATE\"" +
                "}";
        DataWorkerMessage message = DataWorkerMessage.create(jsonString.getBytes());
        assertEquals("wsTest", message.getWorkspaceId());
        assertEquals("visibilitySourceValue", message.getVisibilitySource());
        assertEquals("key1", message.getPropertyKey());
        assertEquals("name1", message.getPropertyName());
        assertEquals(ElementOrPropertyStatus.UPDATE, message.getStatus());
        assertEquals(123456789, message.getBeforeActionTimestamp().longValue());
        assertEquals(Priority.HIGH, message.getPriority());
        assertEquals(2, message.getGraphEdgeId().length);
        assertEquals("e1", message.getGraphEdgeId()[0]);
        assertEquals("e2", message.getGraphEdgeId()[1]);
        assertTrue(
                new JSONObject(message.toJsonString()).toString(2),
                JSONUtil.areEqual(new JSONObject(jsonString), new JSONObject(message.toJsonString()))
        );
    }

    @Test
    public void testSingleEdgeMessage() {
        String jsonString = "{" +
                "  \"propertyKey\": \"key1\"," +
                "  \"graphEdgeId\": \"e1\"," +
                "  \"propertyName\": \"name1\"," +
                "  \"beforeActionTimestamp\": 123456789," +
                "  \"visibilitySource\": \"visibilitySourceValue\"," +
                "  \"priority\": \"HIGH\"," +
                "  \"traceEnabled\": false," +
                "  \"workspaceId\": \"wsTest\"," +
                "  \"status\": \"UPDATE\"" +
                "}";
        DataWorkerMessage message = DataWorkerMessage.create(jsonString.getBytes());
        assertEquals("wsTest", message.getWorkspaceId());
        assertEquals("visibilitySourceValue", message.getVisibilitySource());
        assertEquals("key1", message.getPropertyKey());
        assertEquals("name1", message.getPropertyName());
        assertEquals(ElementOrPropertyStatus.UPDATE, message.getStatus());
        assertEquals(123456789, message.getBeforeActionTimestamp().longValue());
        assertEquals(Priority.HIGH, message.getPriority());
        assertEquals(1, message.getGraphEdgeId().length);
        assertEquals("e1", message.getGraphEdgeId()[0]);
        assertTrue(
                new JSONObject(message.toJsonString()).toString(2),
                JSONUtil.areEqual(new JSONObject(jsonString), new JSONObject(message.toJsonString()))
        );
    }
}
