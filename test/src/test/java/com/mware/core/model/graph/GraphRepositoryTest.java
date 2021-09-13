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
package com.mware.core.model.graph;

import com.google.common.collect.ImmutableSet;
import com.mware.core.config.Configuration;
import com.mware.core.config.ConfigurationLoader;
import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.exception.BcResourceNotFoundException;
import com.mware.core.model.clientapi.dto.ClientApiSourceInfo;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.TestWebQueueRepository;
import com.mware.core.model.workQueue.TestWorkQueueRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.*;
import com.mware.ge.id.QueueIdGenerator;
import com.mware.ge.inmemory.InMemoryGraph;
import com.mware.ge.inmemory.InMemoryGraphConfiguration;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.search.DefaultSearchIndex;
import com.mware.ge.values.storable.Values;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class GraphRepositoryTest {
    private static final String WORKSPACE_ID = "testWorkspaceId";
    private static final String ENTITY_1_VERTEX_ID = "entity1Id";
    private static final Visibility SECRET_VIZ = new BcVisibility(
            Visibility.and(ImmutableSet.of("secret"))).getVisibility();
    private static final Visibility SECRET_AND_WORKSPACE_VIZ = new BcVisibility(
            Visibility.and(ImmutableSet.of("secret", WORKSPACE_ID))).getVisibility();
    private static final Visibility WORKSPACE_VIZ = new Visibility(WORKSPACE_ID);

    private GraphRepository graphRepository;
    private InMemoryGraph graph;

    @Mock
    private User user1;

    @Mock
    private TermMentionRepository termMentionRepository;

    private TestWorkQueueRepository workQueueRepository;
    private TestWebQueueRepository webQueueRepository;

    private Authorizations defaultAuthorizations;
    private VisibilityTranslator visibilityTranslator;

    @Before
    public void setup() throws Exception {
        Map config = new HashMap();
        ConfigurationLoader hashMapConfigurationLoader = new HashMapConfigurationLoader(config);
        Configuration configuration = new Configuration(hashMapConfigurationLoader, new HashMap<>());

        InMemoryGraphConfiguration graphConfig = new InMemoryGraphConfiguration(new HashMap<>());
        QueueIdGenerator idGenerator = new QueueIdGenerator();
        visibilityTranslator = new VisibilityTranslator();
        graph = InMemoryGraph.create(graphConfig, idGenerator, new DefaultSearchIndex(graphConfig));
        defaultAuthorizations = graph.createAuthorizations();
        workQueueRepository = new TestWorkQueueRepository(graph, configuration);
        webQueueRepository = new TestWebQueueRepository();

        graphRepository = new GraphRepository(
                graph,
                termMentionRepository,
                workQueueRepository,
                webQueueRepository,
                configuration
        );
    }

    @Test
    public void testUpdatePropertyVisibilitySource() {
        Authorizations authorizations = graph.createAuthorizations("A");
        Visibility newVisibility = visibilityTranslator.toVisibility("A").getVisibility();

        Vertex v1 = graph.prepareVertex(ENTITY_1_VERTEX_ID, new BcVisibility().getVisibility(), SchemaConstants.CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "p1", stringValue("value1"), new Visibility(""))
                .save(authorizations);

        Property p1 = graphRepository.updatePropertyVisibilitySource(
                v1,
                "k1",
                "p1",
                "",
                "A",
                WORKSPACE_ID,
                user1,
                defaultAuthorizations
        );
        assertEquals(newVisibility, p1.getVisibility());
        graph.flush();

        v1 = graph.getVertex(ENTITY_1_VERTEX_ID, authorizations);
        p1 = v1.getProperty("k1", "p1", newVisibility);
        assertNotNull("could not find p1", p1);
        assertEquals(newVisibility, p1.getVisibility());
        VisibilityJson visibilityJson = BcSchema.VISIBILITY_JSON_METADATA
                .getMetadataValue(p1.getMetadata());
        assertEquals("A", visibilityJson.getSource());
    }

    @Test
    public void testUpdatePropertyVisibilitySourceMissingProperty() {
        Authorizations authorizations = graph.createAuthorizations("A");

        Vertex v1 = graph.prepareVertex(ENTITY_1_VERTEX_ID, new BcVisibility().getVisibility(), SchemaConstants.CONCEPT_TYPE_THING)
                .addPropertyValue("k1", "p1", stringValue("value1"), new Visibility(""))
                .save(authorizations);

        try {
            graphRepository.updatePropertyVisibilitySource(
                    v1,
                    "k1",
                    "pNotFound",
                    "",
                    "A",
                    WORKSPACE_ID,
                    user1,
                    defaultAuthorizations
            );
            fail("expected exception");
        } catch (BcResourceNotFoundException ex) {
            // OK
        }
    }

    @Test
    public void testSetWorkspaceOnlyChangePropertyTwice() {
        Vertex vertex = graph.prepareVertex(ENTITY_1_VERTEX_ID, new BcVisibility().getVisibility(), SchemaConstants.CONCEPT_TYPE_THING)
                .save(defaultAuthorizations);

        final Authorizations workspaceAuthorizations = graph.createAuthorizations(WORKSPACE_ID);

        setProperty(vertex, "newValue1", WORKSPACE_ID, workspaceAuthorizations);

        vertex = graph.getVertex(vertex.getId(), defaultAuthorizations);
        List<Property> properties = toList(vertex.getProperties());
        assertEquals(0, properties.size());

        vertex = graph.getVertex(vertex.getId(), workspaceAuthorizations);
        properties = toList(vertex.getProperties());
        assertEquals(1, properties.size());
        assertEquals("newValue1", properties.get(0).getValue());

        setProperty(vertex, "newValue2", WORKSPACE_ID, workspaceAuthorizations);

        vertex = graph.getVertex(vertex.getId(), defaultAuthorizations);
        properties = toList(vertex.getProperties());
        assertEquals(0, properties.size());

        vertex = graph.getVertex(vertex.getId(), workspaceAuthorizations);
        properties = toList(vertex.getProperties());
        assertEquals(1, properties.size());
        assertEquals("newValue2", properties.get(0).getValue());
    }

    @Test
    public void testSandboxPropertyChangesShouldUpdateSameProperty() {
        final Authorizations authorizations = graph.createAuthorizations("foo", "bar", "baz", WORKSPACE_ID);

        Vertex vertex = graph.prepareVertex(ENTITY_1_VERTEX_ID, new BcVisibility().getVisibility(), SchemaConstants.CONCEPT_TYPE_THING)
                .save(authorizations);

        // new property with visibility
        String propertyValue = "newValue1";
        setProperty(vertex, propertyValue, null, "foo", WORKSPACE_ID, authorizations);

        List<Property> properties = toList(vertex.getProperties());
        Visibility fooVisibility = new BcVisibility(Visibility.and(ImmutableSet.of("foo", WORKSPACE_ID)))
                .getVisibility();
        assertEquals(1, properties.size());
        assertEquals(propertyValue, properties.get(0).getValue());
        assertEquals(fooVisibility, properties.get(0).getVisibility());

        // existing property, new visibility
        setProperty(vertex, propertyValue, "foo", "bar", WORKSPACE_ID, authorizations);

        properties = toList(vertex.getProperties());
        Visibility barVisibility = new BcVisibility(Visibility.and(ImmutableSet.of("bar", WORKSPACE_ID)))
                .getVisibility();
        assertEquals(1, properties.size());
        assertEquals(propertyValue, properties.get(0).getValue());
        assertEquals(barVisibility, properties.get(0).getVisibility());

        // existing property, new value
        propertyValue = "newValue2";
        setProperty(vertex, propertyValue, null, "bar", WORKSPACE_ID, authorizations);

        properties = toList(vertex.getProperties());
        assertEquals(1, properties.size());
        assertEquals(propertyValue, properties.get(0).getValue());
        assertEquals(barVisibility, properties.get(0).getVisibility());

        // existing property, new visibility,  new value
        propertyValue = "newValue3";
        setProperty(vertex, propertyValue, "bar", "baz", WORKSPACE_ID, authorizations);

        properties = toList(vertex.getProperties());
        Visibility bazVisibility = new BcVisibility(Visibility.and(ImmutableSet.of("baz", WORKSPACE_ID)))
                .getVisibility();
        assertEquals(1, properties.size());
        assertEquals(propertyValue, properties.get(0).getValue());
        assertEquals(bazVisibility, properties.get(0).getVisibility());
    }

    @Test
    public void existingPublicPropertySavedWithWorkspaceIsSandboxed() {
        final Authorizations authorizations = graph.createAuthorizations("secret", WORKSPACE_ID);

        Vertex vertex = graph.prepareVertex(ENTITY_1_VERTEX_ID, new BcVisibility().getVisibility(), SchemaConstants.CONCEPT_TYPE_THING)
                .save(authorizations);

        // save property without workspace, which will be public

        String publicValue = "publicValue";
        setProperty(vertex, publicValue, null, "secret", null, authorizations);

        List<Property> properties = toList(vertex.getProperties());

        assertEquals(1, properties.size());
        Property property = properties.get(0);
        assertEquals(publicValue, property.getValue());
        assertEquals(SECRET_VIZ, property.getVisibility());
        assertFalse(property.getHiddenVisibilities().iterator().hasNext());

        // save property with workspace, which will be sandboxed

        String sandboxedValue = "sandboxedValue";
        setProperty(vertex, sandboxedValue, null, "secret", WORKSPACE_ID, authorizations);

        properties = toList(vertex.getProperties());

        assertEquals(2, properties.size());

        property = properties.get(0); // the sandboxed property

        assertEquals(sandboxedValue, property.getValue());
        assertEquals(SECRET_AND_WORKSPACE_VIZ, property.getVisibility());
        assertFalse(property.getHiddenVisibilities().iterator().hasNext());

        property = properties.get(1); // the public property
        Iterator<Visibility> hiddenVisibilities = property.getHiddenVisibilities().iterator();
        assertEquals(publicValue, property.getValue());
        assertEquals(SECRET_VIZ, property.getVisibility());
        assertTrue(hiddenVisibilities.hasNext());
        assertEquals(WORKSPACE_VIZ, hiddenVisibilities.next());

        List<byte[]> queue = workQueueRepository.getWorkQueue(workQueueRepository.getQueueName());
        assertEquals(1, queue.size());
        workQueueRepository.clearQueue();
    }

    @Test
    public void newPropertySavedWithoutWorkspaceIsPublic() {
        final Authorizations authorizations = graph.createAuthorizations("secret");

        Vertex vertex = graph.prepareVertex(ENTITY_1_VERTEX_ID, new BcVisibility().getVisibility(), SchemaConstants.CONCEPT_TYPE_THING)
                .save(authorizations);

        String propertyValue = "newValue";
        setProperty(vertex, propertyValue, null, "secret", null, authorizations);

        List<Property> properties = toList(vertex.getProperties());

        assertEquals(1, properties.size());
        Property property = properties.get(0);
        assertEquals(propertyValue, property.getValue());
        assertEquals(SECRET_VIZ, property.getVisibility());
        assertFalse(property.getHiddenVisibilities().iterator().hasNext());
    }

    @Test
    public void newPropertySavedWithWorkspaceIsSandboxed() {
        final Authorizations authorizations = graph.createAuthorizations("secret", WORKSPACE_ID);

        Vertex vertex = graph.prepareVertex(ENTITY_1_VERTEX_ID, new BcVisibility().getVisibility(), SchemaConstants.CONCEPT_TYPE_THING)
                .save(authorizations);

        String propertyValue = "newValue";
        setProperty(vertex, propertyValue, null, "secret", WORKSPACE_ID, authorizations);

        List<Property> properties = toList(vertex.getProperties());

        assertEquals(1, properties.size());
        Property property = properties.get(0);
        assertEquals(propertyValue, property.getValue());
        assertEquals(SECRET_AND_WORKSPACE_VIZ, property.getVisibility());
        assertFalse(property.getHiddenVisibilities().iterator().hasNext());
    }

    @Test
    public void testBeginGraphUpdate() throws Exception {
        ZonedDateTime modifiedDate = ZonedDateTime.now();
        VisibilityJson visibilityJson = new VisibilityJson();
        PropertyMetadata metadata = new PropertyMetadata(modifiedDate, user1, visibilityJson, new Visibility(""));

        try (GraphUpdateContext ctx = graphRepository.beginGraphUpdate(Priority.NORMAL, user1, defaultAuthorizations)) {
            ElementMutation<Vertex> m = graph.prepareVertex("v1", new Visibility(""), "text#concept1");
            ctx.update(m, modifiedDate, visibilityJson, updateContext -> {
                BcSchema.FILE_NAME.updateProperty(updateContext, "k1", "test1.txt", metadata);
            });

            m = graph.prepareVertex("v2", new Visibility(""), "text#concept1");
            ctx.update(m, updateContext -> {
                updateContext.updateBuiltInProperties(modifiedDate, visibilityJson);
                BcSchema.FILE_NAME.updateProperty(updateContext, "k1", "test2.txt", metadata);
            });
        }

        List<byte[]> queue = workQueueRepository.getWorkQueue(workQueueRepository.getQueueName());
        assertEquals(2, queue.size());
        assertWorkQueueContains(queue, "v1", "", BcSchema.MODIFIED_DATE.getPropertyName());
        assertWorkQueueContains(queue, "v1", "", BcSchema.VISIBILITY_JSON.getPropertyName());
        assertWorkQueueContains(queue, "v1", "k1", BcSchema.FILE_NAME.getPropertyName());
        assertWorkQueueContains(queue, "v2", "", BcSchema.MODIFIED_DATE.getPropertyName());
        assertWorkQueueContains(queue, "v2", "", BcSchema.VISIBILITY_JSON.getPropertyName());
        assertWorkQueueContains(queue, "v2", "k1", BcSchema.FILE_NAME.getPropertyName());

        Vertex v1 = graph.getVertex("v1", defaultAuthorizations);
        assertEquals("test1.txt", BcSchema.FILE_NAME.getFirstPropertyValue(v1));
        assertEquals("text#concept1", v1.getConceptType());
        assertEquals(modifiedDate, BcSchema.MODIFIED_DATE.getPropertyValue(v1));
        assertEquals(user1.getUserId(), BcSchema.MODIFIED_BY.getPropertyValue(v1));
        assertEquals(visibilityJson, BcSchema.VISIBILITY_JSON.getPropertyValue(v1));
    }

    private void assertWorkQueueContains(List<byte[]> queue, String vertexId, String propertyKey, String propertyName) {
        for (byte[] item : queue) {
            JSONObject json = new JSONObject(new String(item));
            JSONArray properties = json.getJSONArray("properties");
            for (int i = 0; i < properties.length(); i++) {
                JSONObject property = properties.getJSONObject(i);
                if (json.getString("graphVertexId").equals(vertexId)
                        && property.getString("propertyKey").equals(propertyKey)
                        && property.getString("propertyName").equals(propertyName)) {
                    return;
                }
            }
        }
        fail("Could not find queue item " + vertexId + ", " + propertyKey + ", " + propertyName);
    }

    private void setProperty(Vertex vertex, String value, String workspaceId, Authorizations workspaceAuthorizations) {
        setProperty(vertex, value, "", "", workspaceId, workspaceAuthorizations);
    }

    private void setProperty(
            Vertex vertex, String value, String oldVisibility, String newVisibility,
            String workspaceId, Authorizations workspaceAuthorizations
    ) {
        VisibilityAndElementMutation<Vertex> setPropertyResult = graphRepository.setProperty(
                vertex,
                "prop1",
                "key1",
                Values.stringValue(value),
                Metadata.create(),
                oldVisibility,
                newVisibility,
                workspaceId,
                "I changed it",
                new ClientApiSourceInfo(),
                user1,
                workspaceAuthorizations
        );
        setPropertyResult.elementMutation.save(workspaceAuthorizations);
        graph.flush();
    }
}


