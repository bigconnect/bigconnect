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
package com.mware.core.model.schema;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.mware.core.GraphTestBase;
import com.mware.core.cache.CacheOptions;
import com.mware.core.exception.BcAccessDeniedException;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.*;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.model.workspace.Workspace;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.ge.*;
import com.mware.ge.util.GeAssert;
import com.mware.ge.util.IterableUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;
import static com.mware.core.model.schema.SchemaFactory.RESOURCE_ENTITY_PNG;
import static com.mware.core.model.schema.SchemaRepository.PUBLIC;
import static com.mware.ge.values.storable.Values.stringValue;
import static org.junit.Assert.*;

public abstract class SchemaRepositoryTestBase extends GraphTestBase {
    private static final String GLYPH_ICON_FILE = "glyphicons_003_user@2x.png";

    private static final String SANDBOX_CONCEPT_NAME = "sandbox-concept-iri";
    private static final String SANDBOX_RELATIONSHIP_NAME = "sandbox-relationship-iri";
    private static final String SANDBOX_PROPERTY_NAME = "sandbox-property-iri";
    private static final String SANDBOX_PROPERTY_NAME_ONLY_SANDBOXED_CONCEPT = "sandbox-property-iri2";
    private static final String SANDBOX_DISPLAY_NAME = "Sandbox Display";
    private static final String PUBLIC_CONCEPT_NAME = "public-concept-iri";
    private static final String PUBLIC_RELATIONSHIP_NAME = "public-relationship-iri";
    private static final String PUBLIC_PROPERTY_NAME = "public-property-iri";
    private static final String PUBLIC_DISPLAY_NAME = "Public Display";

    private String workspaceId = "junit-workspace";
    private User systemUser = new SystemUser();

    private Authorizations authorizations;
    private User user;
    private User adminUser;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() throws Exception {
        super.before();
        authorizations = getGraph().createAuthorizations();
        user = getUserRepository().findOrAddUser("junit", "Junit", "junit@example.com", "password");
        Workspace workspace = getWorkspaceRepository().add(workspaceId, "Junit Workspace", user);
        if (getPrivilegeRepository().hasPrivilege(user, Privilege.ADMIN)) {
            fail("User shouldn't have admin");
        }

        adminUser = getUserRepository().findOrAddUser("junit-admin", "Junit Admin", "junit-admin@example.com", "password");
        Set<String> privileges = Privilege.ALL_BUILT_IN.stream().map(Privilege::getName).collect(Collectors.toSet());
        setPrivileges(adminUser, privileges);

        getWorkspaceRepository().updateUserOnWorkspace(workspace, adminUser.getUserId(), WorkspaceAccess.WRITE, systemUser);
    }

    @Test
    public void testChangingDisplayAnnotationsShouldSucceed() throws Exception {
        loadTestSchema();

        createTestChangedSchema();

        validateChangedOwlRelationships();
        validateChangedOwlConcepts();
        validateChangedOwlProperties();
    }

    @Test
    public void testGettingParentConceptReturnsParentProperties() throws Exception {
        loadHierarchySchema();
        Concept concept = getSchemaRepository().getConceptByName("testhierarchy#person", PUBLIC);
        Concept parentConcept = getSchemaRepository().getParentConcept(concept, PUBLIC);
        assertEquals(1, parentConcept.getProperties().size());
    }

    @Test
    public void testRelationshipHierarchy() throws Exception {
        loadHierarchySchema();

        Relationship relationship = getSchemaRepository().getRelationshipByName("testhierarchy#personReallyKnowsPerson", PUBLIC);
        assertEquals("testhierarchy#personKnowsPerson", relationship.getParentName());

        relationship = getSchemaRepository().getParentRelationship(relationship, PUBLIC);
        assertEquals("testhierarchy#personKnowsPerson", relationship.getName());
        assertEquals(SchemaRepositoryBase.TOP_OBJECT_PROPERTY_NAME, relationship.getParentName());
    }

    @Test
    public void testGenerateIri() throws Exception {
        assertEquals("Should lowercase", "xxx#90b271732810a0498f226ab8f38e88e7b0d711af", getSchemaRepository().generateDynamicName(Concept.class, "XxX", "w0"));
        assertEquals("Extended data should change hash", "xxx#382894aad1a1a2971aea0ed0575ba216efc173a9", getSchemaRepository().generateDynamicName(Concept.class, "XxX", "w0", "1"));
        assertEquals("replace spaces", "s123#323dd4ea6d1e9d5c0d2e88bced0b11628513bd81", getSchemaRepository().generateDynamicName(Concept.class, " S 1 2 3 ", "w0"));
        assertEquals("replace non-alpha-num", "aa1#29c418a20c565a33dc430bfe64b3c9e092499225", getSchemaRepository().generateDynamicName(Concept.class, "a !@#A$%1^&*()<>?\":{}=+),[]\\|`~", "w0"));


        StringBuilder valid = new StringBuilder();
        StringBuilder invalid = new StringBuilder();
        for (int i = 0; i < SchemaRepositoryBase.MAX_DISPLAY_NAME + 2; i++) {
            if (i < SchemaRepositoryBase.MAX_DISPLAY_NAME) valid.append("a");
            invalid.append("a");
        }
        assertEquals("length check valid", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#e484241fdd71fd12c26998ec4cca4c65ae766af6", getSchemaRepository().generateDynamicName(Concept.class, valid.toString(), "w0"));
        assertEquals("length/hash check invalid", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#e484241fdd71fd12c26998ec4cca4c65ae766af6", getSchemaRepository().generateDynamicName(Concept.class, invalid.toString(), "w0"));
    }

    @Test
    public void testGetConceptsWithProperties() throws Exception {
        loadHierarchySchema();
        getSchemaRepository().clearCache();

        Iterable<Concept> conceptsWithProperties = getSchemaRepository().getConceptsWithProperties(workspaceId);
        Map<String, Concept> conceptsByName = StreamSupport.stream(conceptsWithProperties.spliterator(), false)
                .collect(Collectors.toMap(Concept::getName, Function.identity()));

        Concept personConcept = conceptsByName.get("testhierarchy#person");

        // Check parent names
        assertEquals("thing", conceptsByName.get("testhierarchy#contact").getParentConceptName());
        assertEquals("testhierarchy#contact", personConcept.getParentConceptName());

        // Check properties
        List<SchemaProperty> personProperties = new ArrayList<>(personConcept.getProperties());
        assertEquals(4, personProperties.size());
        assertTrue(personProperties.stream().anyMatch(p -> "testhierarchy#middleName".equals(p.getName())));

        // Check intents
        List<String> intents = Arrays.asList(personConcept.getIntents());
        assertEquals(2, intents.size());
        assertTrue(intents.contains("face"));
        assertTrue(intents.contains("person"));

        // Spot check other concept values
        assertEquals("Person", personConcept.getDisplayName());
        assertEquals("prop('testhierarchy#name') || ''", personConcept.getTitleFormula());
    }

    @Test
    public void testGetRelationships() throws Exception {
        loadHierarchySchema();
        getSchemaRepository().clearCache();

        Iterable<Relationship> relationships = getSchemaRepository().getRelationships(workspaceId);
        Map<String, Relationship> relationshipsByName = StreamSupport.stream(relationships.spliterator(), false)
                .collect(Collectors.toMap(Relationship::getName, Function.identity()));

        assertNull(relationshipsByName.get("topObjectProperty").getParentName());
        assertEquals("topObjectProperty", relationshipsByName.get("testhierarchy#personKnowsPerson").getParentName());
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testExceptionWhenDeletingPublicConcepts() throws Exception {
        createSampleOntology();
        getSchemaRepository().deleteConcept(PUBLIC_CONCEPT_NAME, user, null);
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testExceptionWhenDeletingSandboxedConceptsAsNonAdmin() throws Exception {
        createSampleOntology();
        getSchemaRepository().deleteConcept(SANDBOX_CONCEPT_NAME, user, workspaceId);
    }

    @Test
    public void testExceptionDeletingSandboxedConceptsWithVertices() throws Exception {
        createSampleOntology();
        Concept concept = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, workspaceId);
        assertTrue("Concept exists", concept != null && concept.getName().equals(SANDBOX_CONCEPT_NAME));

        VisibilityJson json = VisibilityJson.updateVisibilitySourceAndAddWorkspaceId(new VisibilityJson(), "", workspaceId);
        Visibility visibility = getVisibilityTranslator().toVisibility(json).getVisibility();
        VertexBuilder vb = getGraph().prepareVertex(visibility, SANDBOX_CONCEPT_NAME);
        vb.save(authorizations);

        thrown.expect(BcException.class);
        thrown.expectMessage("Unable to delete concept that have vertices assigned to it");
        getSchemaRepository().deleteConcept(SANDBOX_CONCEPT_NAME, adminUser, workspaceId);
    }

    @Test
    public void testExceptionDeletingSandboxedConceptsWithDescendants() throws Exception {
        createSampleOntology();
        Concept concept = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, workspaceId);

        // Add a descendant
        getSchemaRepository().getOrCreateConcept(concept, SANDBOX_CONCEPT_NAME + "child", SANDBOX_DISPLAY_NAME, systemUser, workspaceId);
        getSchemaRepository().clearCache(workspaceId);

        thrown.expect(BcException.class);
        thrown.expectMessage("Unable to delete concept that have children");
        getSchemaRepository().deleteConcept(SANDBOX_CONCEPT_NAME, adminUser, workspaceId);
    }

    @Test
    public void testExceptionDeletingSandboxedConceptsWithRelationshipsDomain() throws Exception {
        createSampleOntology();
        Concept concept = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, workspaceId);

        // Add an edge type
        List<Concept> things = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        getSchemaRepository().getOrCreateRelationshipType(null, Arrays.asList(concept), things, "sandboxed-relationship-withsandboxed-concepts", true, true, adminUser, workspaceId);
        getSchemaRepository().clearCache(workspaceId);

        thrown.expect(BcException.class);
        thrown.expectMessage("Unable to delete concept that is used in domain/range of relationship");
        getSchemaRepository().deleteConcept(SANDBOX_CONCEPT_NAME, adminUser, workspaceId);
    }

    @Test
    public void testExceptionDeletingSandboxedConceptsWithRelationshipsRange() throws Exception {
        createSampleOntology();
        Concept concept = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, workspaceId);

        // Add an edge type
        List<Concept> things = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        getSchemaRepository().getOrCreateRelationshipType(null, things, Arrays.asList(concept), "sandboxed-relationship-withsandboxed-concepts", true, true, adminUser, workspaceId);
        getSchemaRepository().clearCache(workspaceId);

        thrown.expect(BcException.class);
        thrown.expectMessage("Unable to delete concept that is used in domain/range of relationship");
        getSchemaRepository().deleteConcept(SANDBOX_CONCEPT_NAME, adminUser, workspaceId);
    }

    @Test
    public void testDeletingSandboxedConcepts() throws Exception {
        createSampleOntology();
        Concept concept = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, workspaceId);
        assertTrue("Concept exists", concept != null && concept.getName().equals(SANDBOX_CONCEPT_NAME));

        SchemaProperty property = getSchemaRepository().getPropertyByName(SANDBOX_PROPERTY_NAME_ONLY_SANDBOXED_CONCEPT, workspaceId);
        assertTrue("Property exists", property != null && property.getName().equals(SANDBOX_PROPERTY_NAME_ONLY_SANDBOXED_CONCEPT));
        assertTrue("Concept has property", concept.getProperties().stream().anyMatch(ontologyProperty -> ontologyProperty.getName().equals(SANDBOX_PROPERTY_NAME_ONLY_SANDBOXED_CONCEPT)));

        getSchemaRepository().deleteConcept(SANDBOX_CONCEPT_NAME, adminUser, workspaceId);
        getSchemaRepository().clearCache(workspaceId);

        concept = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, workspaceId);
        assertTrue("Concept should have been deleted", concept == null);

        property = getSchemaRepository().getPropertyByName(SANDBOX_PROPERTY_NAME_ONLY_SANDBOXED_CONCEPT, workspaceId);
        assertTrue("Property only used in this concept is deleted", property == null);

        property = getSchemaRepository().getPropertyByName(SANDBOX_PROPERTY_NAME, workspaceId);
        assertTrue("Property used in other concepts is updated", property != null);
        assertEquals(1, property.getConceptNames().size());
        assertEquals(PUBLIC_CONCEPT_NAME, property.getConceptNames().get(0));
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testExceptionWhenDeletingPublicProperties() throws Exception {
        createSampleOntology();
        getSchemaRepository().deleteProperty(PUBLIC_PROPERTY_NAME, user, null);
    }

    @Test
    public void testExceptionDeletingSandboxedPropertiesWithVertices() throws Exception {
        createSampleOntology();

        VisibilityJson json = VisibilityJson.updateVisibilitySourceAndAddWorkspaceId(new VisibilityJson(), "", workspaceId);
        Visibility visibility = getVisibilityTranslator().toVisibility(json).getVisibility();
        VertexBuilder vb = getGraph().prepareVertex(visibility, CONCEPT_TYPE_THING);
        vb.setProperty(SANDBOX_PROPERTY_NAME, stringValue("a value"), visibility);
        vb.save(authorizations);

        thrown.expect(BcException.class);
        thrown.expectMessage("Unable to delete property that have elements using it");
        getSchemaRepository().deleteProperty(SANDBOX_PROPERTY_NAME, adminUser, workspaceId);
    }

    @Test
    public void testExceptionDeletingSandboxedPropertiesWithEdges() throws Exception {
        createSampleOntology();

        VisibilityJson json = VisibilityJson.updateVisibilitySourceAndAddWorkspaceId(new VisibilityJson(), "", workspaceId);
        Visibility visibility = getVisibilityTranslator().toVisibility(json).getVisibility();

        Vertex source = getGraph().prepareVertex(visibility, CONCEPT_TYPE_THING).save(authorizations);
        Vertex target = getGraph().prepareVertex(visibility, CONCEPT_TYPE_THING).save(authorizations);

        EdgeBuilder edgeBuilder = getGraph().prepareEdge(source, target, SANDBOX_RELATIONSHIP_NAME, visibility);
        edgeBuilder.setProperty(SANDBOX_PROPERTY_NAME, stringValue("a value"), visibility);
        edgeBuilder.save(authorizations);

        thrown.expect(BcException.class);
        thrown.expectMessage("Unable to delete property that have elements using it");
        getSchemaRepository().deleteProperty(SANDBOX_PROPERTY_NAME, adminUser, workspaceId);
    }

    @Test
    public void testDeletingProperties() throws Exception {
        createSampleOntology();
        SchemaProperty property = getSchemaRepository().getPropertyByName(SANDBOX_PROPERTY_NAME, workspaceId);
        assertNotNull("Property exists", property);
        getSchemaRepository().deleteProperty(SANDBOX_PROPERTY_NAME, adminUser, workspaceId);
        getSchemaRepository().clearCache(workspaceId);

        property = getSchemaRepository().getPropertyByName(SANDBOX_PROPERTY_NAME, workspaceId);
        assertNull("Property is deleted", property);
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testExceptionWhenDeletingPublicRelationships() throws Exception {
        createSampleOntology();
        getSchemaRepository().deleteRelationship(PUBLIC_RELATIONSHIP_NAME, user, null);
    }

    @Test
    public void testExceptionDeletingSandboxedRelationshipsWithEdges() throws Exception {
        createSampleOntology();

        VisibilityJson json = VisibilityJson.updateVisibilitySourceAndAddWorkspaceId(new VisibilityJson(), "", workspaceId);
        Visibility visibility = getVisibilityTranslator().toVisibility(json).getVisibility();

        Vertex source = getGraph().prepareVertex(visibility, SchemaConstants.CONCEPT_TYPE_THING).save(authorizations);
        Vertex target = getGraph().prepareVertex(visibility, SchemaConstants.CONCEPT_TYPE_THING).save(authorizations);
        EdgeBuilder edgeBuilder = getGraph().prepareEdge(source, target, SANDBOX_RELATIONSHIP_NAME, visibility);
        edgeBuilder.setProperty(SANDBOX_PROPERTY_NAME, stringValue("a value"), visibility);
        edgeBuilder.save(authorizations);

        thrown.expect(BcException.class);
        thrown.expectMessage("Unable to delete relationship that have edges using it");
        getSchemaRepository().deleteRelationship(SANDBOX_RELATIONSHIP_NAME, adminUser, workspaceId);
    }

    @Test
    public void testExceptionDeletingSandboxedRelationshipsWithDescendants() throws Exception {
        createSampleOntology();

        Relationship relationship = getSchemaRepository().getRelationshipByName(SANDBOX_RELATIONSHIP_NAME, workspaceId);

        VisibilityJson.updateVisibilitySourceAndAddWorkspaceId(new VisibilityJson(), "", workspaceId);

        List<Concept> things = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        getSchemaRepository().getOrCreateRelationshipType(relationship, things, things, SANDBOX_RELATIONSHIP_NAME + "child", true, true, adminUser, workspaceId);
        getSchemaRepository().clearCache(workspaceId);

        thrown.expect(BcException.class);
        thrown.expectMessage("Unable to delete relationship that have children");
        getSchemaRepository().deleteRelationship(SANDBOX_RELATIONSHIP_NAME, adminUser, workspaceId);
    }

    @Test
    public void testDeletingSandboxedRelationships() throws Exception {
        createSampleOntology();
        Relationship relationship = getSchemaRepository().getRelationshipByName(SANDBOX_RELATIONSHIP_NAME, workspaceId);
        String propertyThatShouldBeDeleted = SANDBOX_PROPERTY_NAME + ".relationship";

        createProperty(propertyThatShouldBeDeleted, SANDBOX_DISPLAY_NAME, Collections.emptyList(), Collections.singletonList(relationship), workspaceId);
        getSchemaRepository().clearCache(workspaceId);

        relationship = getSchemaRepository().getRelationshipByName(SANDBOX_RELATIONSHIP_NAME, workspaceId);

        SchemaProperty property = getSchemaRepository().getPropertyByName(propertyThatShouldBeDeleted, workspaceId);
        assertTrue("Property exists", property != null && property.getName().equals(propertyThatShouldBeDeleted));
        assertTrue("Relationship has property", relationship.getProperties().stream().anyMatch(ontologyProperty -> ontologyProperty.getName().equals(propertyThatShouldBeDeleted)));

        getSchemaRepository().deleteRelationship(SANDBOX_RELATIONSHIP_NAME, adminUser, workspaceId);
        getSchemaRepository().clearCache(workspaceId);

        relationship = getSchemaRepository().getRelationshipByName(SANDBOX_RELATIONSHIP_NAME, workspaceId);
        assertNull("Relationship should have been deleted", relationship);

        property = getSchemaRepository().getPropertyByName(propertyThatShouldBeDeleted, workspaceId);
        assertNull("Property only used in this relationship is deleted", property);

        property = getSchemaRepository().getPropertyByName(SANDBOX_PROPERTY_NAME, workspaceId);
        assertNotNull("Property used in other relationships is updated", property);
        assertEquals(1, property.getRelationshipNames().size());
        assertEquals(PUBLIC_RELATIONSHIP_NAME, property.getRelationshipNames().get(0));
    }

    @Test
    public void testGetConceptsByName() throws Exception {
        createSampleOntology();

        Iterable<Concept> conceptsByName = getSchemaRepository().getConceptsByName(Collections.singletonList(PUBLIC_CONCEPT_NAME), PUBLIC);
        List<Concept> concepts = IterableUtils.toList(conceptsByName);
        assertEquals(1, concepts.size());
        assertEquals(PUBLIC_CONCEPT_NAME, concepts.get(0).getName());

        conceptsByName = getSchemaRepository().getConceptsByName(Collections.singletonList(SANDBOX_CONCEPT_NAME), workspaceId);
        concepts = IterableUtils.toList(conceptsByName);
        assertEquals(1, concepts.size());
        assertEquals(SANDBOX_CONCEPT_NAME, concepts.get(0).getName());

        conceptsByName = getSchemaRepository().getConceptsByName(Arrays.asList(PUBLIC_CONCEPT_NAME, SANDBOX_CONCEPT_NAME), PUBLIC);
        concepts = IterableUtils.toList(conceptsByName);
        assertEquals(1, concepts.size());
        assertEquals(PUBLIC_CONCEPT_NAME, concepts.get(0).getName());

        conceptsByName = getSchemaRepository().getConceptsByName(Arrays.asList(PUBLIC_CONCEPT_NAME, SANDBOX_CONCEPT_NAME), workspaceId);
        concepts = IterableUtils.toList(conceptsByName);
        assertEquals(2, concepts.size());
        assertTrue(concepts.stream().map(Concept::getName).anyMatch(name -> name.equals(PUBLIC_CONCEPT_NAME)));
        assertTrue(concepts.stream().map(Concept::getName).anyMatch(name -> name.equals(SANDBOX_CONCEPT_NAME)));
    }

    @Test
    public void testGetConceptsById() throws Exception {
        SampleSchemaDetails sampleSchemaDetails = createSampleOntology();

        Iterable<Concept> conceptsByName = getSchemaRepository().getConcepts(Collections.singletonList(sampleSchemaDetails.publicConceptId), PUBLIC);
        List<Concept> concepts = IterableUtils.toList(conceptsByName);
        assertEquals(1, concepts.size());
        assertEquals(PUBLIC_CONCEPT_NAME, concepts.get(0).getName());

        conceptsByName = getSchemaRepository().getConcepts(Collections.singletonList(sampleSchemaDetails.sandboxConceptId), workspaceId);
        concepts = IterableUtils.toList(conceptsByName);
        assertEquals(1, concepts.size());
        assertEquals(SANDBOX_CONCEPT_NAME, concepts.get(0).getName());

        conceptsByName = getSchemaRepository().getConcepts(Arrays.asList(sampleSchemaDetails.publicConceptId, sampleSchemaDetails.sandboxConceptId), PUBLIC);
        concepts = IterableUtils.toList(conceptsByName);
        assertEquals(1, concepts.size());
        assertEquals(PUBLIC_CONCEPT_NAME, concepts.get(0).getName());

        conceptsByName = getSchemaRepository().getConcepts(Arrays.asList(sampleSchemaDetails.publicConceptId, sampleSchemaDetails.sandboxConceptId), workspaceId);
        concepts = IterableUtils.toList(conceptsByName);
        assertEquals(2, concepts.size());
        assertTrue(concepts.stream().map(Concept::getName).anyMatch(name -> name.equals(PUBLIC_CONCEPT_NAME)));
        assertTrue(concepts.stream().map(Concept::getName).anyMatch(name -> name.equals(SANDBOX_CONCEPT_NAME)));
    }

    @Test
    public void testGetRelationshipsByName() throws Exception {
        createSampleOntology();

        Iterable<Relationship> relationshipsByName = getSchemaRepository().getRelationshipsByName(Collections.singletonList(PUBLIC_RELATIONSHIP_NAME), PUBLIC);
        List<Relationship> relationships = IterableUtils.toList(relationshipsByName);
        assertEquals(1, relationships.size());
        assertEquals(PUBLIC_RELATIONSHIP_NAME, relationships.get(0).getName());

        relationshipsByName = getSchemaRepository().getRelationshipsByName(Collections.singletonList(SANDBOX_RELATIONSHIP_NAME), workspaceId);
        relationships = IterableUtils.toList(relationshipsByName);
        assertEquals(1, relationships.size());
        assertEquals(SANDBOX_RELATIONSHIP_NAME, relationships.get(0).getName());

        relationshipsByName = getSchemaRepository().getRelationshipsByName(Arrays.asList(PUBLIC_RELATIONSHIP_NAME, SANDBOX_RELATIONSHIP_NAME), PUBLIC);
        relationships = IterableUtils.toList(relationshipsByName);
        assertEquals(1, relationships.size());
        assertEquals(PUBLIC_RELATIONSHIP_NAME, relationships.get(0).getName());

        relationshipsByName = getSchemaRepository().getRelationshipsByName(Arrays.asList(PUBLIC_RELATIONSHIP_NAME, SANDBOX_RELATIONSHIP_NAME), workspaceId);
        relationships = IterableUtils.toList(relationshipsByName);
        assertEquals(2, relationships.size());
        assertTrue(relationships.stream().map(Relationship::getName).anyMatch(name -> name.equals(PUBLIC_RELATIONSHIP_NAME)));
        assertTrue(relationships.stream().map(Relationship::getName).anyMatch(name -> name.equals(SANDBOX_RELATIONSHIP_NAME)));
    }

    @Test
    public void testGetRelationshipsById() throws Exception {
        SampleSchemaDetails sampleSchemaDetails = createSampleOntology();

        Iterable<Relationship> relationshipsByName = getSchemaRepository().getRelationships(Collections.singletonList(sampleSchemaDetails.publicRelationshipId), PUBLIC);
        List<Relationship> relationships = IterableUtils.toList(relationshipsByName);
        assertEquals(1, relationships.size());
        assertEquals(PUBLIC_RELATIONSHIP_NAME, relationships.get(0).getName());

        relationshipsByName = getSchemaRepository().getRelationships(Collections.singletonList(sampleSchemaDetails.sandboxRelationshipId), workspaceId);
        relationships = IterableUtils.toList(relationshipsByName);
        assertEquals(1, relationships.size());
        assertEquals(SANDBOX_RELATIONSHIP_NAME, relationships.get(0).getName());

        relationshipsByName = getSchemaRepository().getRelationships(Arrays.asList(sampleSchemaDetails.publicRelationshipId, sampleSchemaDetails.sandboxRelationshipId), PUBLIC);
        relationships = IterableUtils.toList(relationshipsByName);
        assertEquals(1, relationships.size());
        assertEquals(PUBLIC_RELATIONSHIP_NAME, relationships.get(0).getName());

        relationshipsByName = getSchemaRepository().getRelationships(Arrays.asList(sampleSchemaDetails.publicRelationshipId, sampleSchemaDetails.sandboxRelationshipId), workspaceId);
        relationships = IterableUtils.toList(relationshipsByName);
        assertEquals(2, relationships.size());
        assertTrue(relationships.stream().map(Relationship::getName).anyMatch(name -> name.equals(PUBLIC_RELATIONSHIP_NAME)));
        assertTrue(relationships.stream().map(Relationship::getName).anyMatch(name -> name.equals(SANDBOX_RELATIONSHIP_NAME)));
    }

    @Test
    public void testPropertiesByName() throws Exception {
        createSampleOntology();

        Iterable<SchemaProperty> propertiesByName = getSchemaRepository().getPropertiesByName(Collections.singletonList(PUBLIC_PROPERTY_NAME), PUBLIC);
        List<SchemaProperty> properties = IterableUtils.toList(propertiesByName);
        assertEquals(1, properties.size());
        assertEquals(PUBLIC_PROPERTY_NAME, properties.get(0).getName());

        propertiesByName = getSchemaRepository().getPropertiesByName(Collections.singletonList(SANDBOX_PROPERTY_NAME), workspaceId);
        properties = IterableUtils.toList(propertiesByName);
        assertEquals(1, properties.size());
        assertEquals(SANDBOX_PROPERTY_NAME, properties.get(0).getName());

        propertiesByName = getSchemaRepository().getPropertiesByName(Arrays.asList(PUBLIC_PROPERTY_NAME, SANDBOX_PROPERTY_NAME), PUBLIC);
        properties = IterableUtils.toList(propertiesByName);
        assertEquals(1, properties.size());
        assertEquals(PUBLIC_PROPERTY_NAME, properties.get(0).getName());

        propertiesByName = getSchemaRepository().getPropertiesByName(Arrays.asList(PUBLIC_PROPERTY_NAME, SANDBOX_PROPERTY_NAME), workspaceId);
        properties = IterableUtils.toList(propertiesByName);
        assertEquals(2, properties.size());
        assertTrue(properties.stream().map(SchemaProperty::getName).anyMatch(name -> name.equals(PUBLIC_PROPERTY_NAME)));
        assertTrue(properties.stream().map(SchemaProperty::getName).anyMatch(name -> name.equals(SANDBOX_PROPERTY_NAME)));
    }

    @Test
    public void testPropertiesById() throws Exception {
        SampleSchemaDetails sampleSchemaDetails = createSampleOntology();

        Iterable<SchemaProperty> propertiesByName = getSchemaRepository().getProperties(Collections.singletonList(sampleSchemaDetails.publicPropertyId), PUBLIC);
        List<SchemaProperty> properties = IterableUtils.toList(propertiesByName);
        assertEquals(1, properties.size());
        assertEquals(PUBLIC_PROPERTY_NAME, properties.get(0).getName());

        propertiesByName = getSchemaRepository().getProperties(Collections.singletonList(sampleSchemaDetails.sandboxPropertyId), workspaceId);
        properties = IterableUtils.toList(propertiesByName);
        assertEquals(1, properties.size());
        assertEquals(SANDBOX_PROPERTY_NAME, properties.get(0).getName());

        propertiesByName = getSchemaRepository().getProperties(Arrays.asList(sampleSchemaDetails.publicPropertyId, sampleSchemaDetails.sandboxPropertyId), PUBLIC);
        properties = IterableUtils.toList(propertiesByName);
        assertEquals(1, properties.size());
        assertEquals(PUBLIC_PROPERTY_NAME, properties.get(0).getName());

        propertiesByName = getSchemaRepository().getProperties(Arrays.asList(sampleSchemaDetails.publicPropertyId, sampleSchemaDetails.sandboxPropertyId), workspaceId);
        properties = IterableUtils.toList(propertiesByName);
        assertEquals(2, properties.size());
        assertTrue(properties.stream().map(SchemaProperty::getName).anyMatch(name -> name.equals(PUBLIC_PROPERTY_NAME)));
        assertTrue(properties.stream().map(SchemaProperty::getName).anyMatch(name -> name.equals(SANDBOX_PROPERTY_NAME)));
    }

    @Test
    public void testClientApiObjectWithUnknownWorkspace() throws Exception {
        createSampleOntology();

        ClientApiSchema clientApiObject = getSchemaRepository().getClientApiObject("unknown-workspace");

        assertFalse(clientApiObject.getConcepts().stream().anyMatch(concept -> concept.getTitle().equals(SANDBOX_CONCEPT_NAME)));
        ClientApiSchema.Concept publicApiConcept = clientApiObject.getConcepts().stream()
                .filter(concept -> concept.getTitle().equals(PUBLIC_CONCEPT_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load public concept"));
        assertEquals(SandboxStatus.PUBLIC, publicApiConcept.getSandboxStatus());

        assertFalse(clientApiObject.getRelationships().stream().anyMatch(relationship -> relationship.getTitle().equals(SANDBOX_RELATIONSHIP_NAME)));
        ClientApiSchema.Relationship publicApiRelationship = clientApiObject.getRelationships().stream()
                .filter(relationship -> relationship.getTitle().equals(PUBLIC_RELATIONSHIP_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load public relationship"));
        assertEquals(SandboxStatus.PUBLIC, publicApiRelationship.getSandboxStatus());

        assertFalse(clientApiObject.getProperties().stream().anyMatch(property -> property.getTitle().equals(SANDBOX_PROPERTY_NAME)));
        ClientApiSchema.Property publicApiProperty = clientApiObject.getProperties().stream()
                .filter(property -> property.getTitle().equals(PUBLIC_PROPERTY_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load public property"));
        assertEquals(SandboxStatus.PUBLIC, publicApiProperty.getSandboxStatus());
    }

    @Test
    public void testClientApiObjectWithNoWorkspace() throws Exception {
        createSampleOntology();

        ClientApiSchema clientApiObject = getSchemaRepository().getClientApiObject(PUBLIC);

        assertFalse(clientApiObject.getConcepts().stream().anyMatch(concept -> concept.getTitle().equals(SANDBOX_CONCEPT_NAME)));
        ClientApiSchema.Concept publicApiConcept = clientApiObject.getConcepts().stream()
                .filter(concept -> concept.getTitle().equals(PUBLIC_CONCEPT_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load public concept"));
        assertEquals(SandboxStatus.PUBLIC, publicApiConcept.getSandboxStatus());

        assertFalse(clientApiObject.getRelationships().stream().anyMatch(relationship -> relationship.getTitle().equals(SANDBOX_RELATIONSHIP_NAME)));
        ClientApiSchema.Relationship publicApiRelationship = clientApiObject.getRelationships().stream()
                .filter(relationship -> relationship.getTitle().equals(PUBLIC_RELATIONSHIP_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load public relationship"));
        assertEquals(SandboxStatus.PUBLIC, publicApiRelationship.getSandboxStatus());

        assertFalse(clientApiObject.getProperties().stream().anyMatch(property -> property.getTitle().equals(SANDBOX_PROPERTY_NAME)));
        ClientApiSchema.Property publicApiProperty = clientApiObject.getProperties().stream()
                .filter(property -> property.getTitle().equals(PUBLIC_PROPERTY_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load public property"));
        assertEquals(SandboxStatus.PUBLIC, publicApiProperty.getSandboxStatus());

        // ensure the sandboxed property appears on all the proper components
        assertEquals(1, publicApiConcept.getProperties().size());
        assertEquals(PUBLIC_PROPERTY_NAME, publicApiConcept.getProperties().get(0));
        assertEquals(1, publicApiRelationship.getProperties().size());
        assertEquals(PUBLIC_PROPERTY_NAME, publicApiRelationship.getProperties().get(0));
    }

    @Test
    public void testClientApiObjectWithValidWorkspace() throws Exception {
        createSampleOntology();

        ClientApiSchema clientApiObject = getSchemaRepository().getClientApiObject(workspaceId);

        ClientApiSchema.Concept sandboxApiConcept = clientApiObject.getConcepts().stream()
                .filter(concept -> concept.getTitle().equals(SANDBOX_CONCEPT_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load sandbox concept"));
        assertEquals(SandboxStatus.PRIVATE, sandboxApiConcept.getSandboxStatus());
        ClientApiSchema.Concept publicApiConcept = clientApiObject.getConcepts().stream()
                .filter(concept -> concept.getTitle().equals(PUBLIC_CONCEPT_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load public concept"));
        assertEquals(SandboxStatus.PUBLIC, publicApiConcept.getSandboxStatus());

        ClientApiSchema.Relationship sandboxApiRelationship = clientApiObject.getRelationships().stream()
                .filter(relationship -> relationship.getTitle().equals(SANDBOX_RELATIONSHIP_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load sandbox relationship"));
        assertEquals(SandboxStatus.PRIVATE, sandboxApiRelationship.getSandboxStatus());
        ClientApiSchema.Relationship publicApiRelationship = clientApiObject.getRelationships().stream()
                .filter(relationship -> relationship.getTitle().equals(PUBLIC_RELATIONSHIP_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load public relationship"));
        assertEquals(SandboxStatus.PUBLIC, publicApiRelationship.getSandboxStatus());

        ClientApiSchema.Property sandboxApiProperty = clientApiObject.getProperties().stream()
                .filter(property -> property.getTitle().equals(SANDBOX_PROPERTY_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load sandbox property"));
        assertEquals(SandboxStatus.PRIVATE, sandboxApiProperty.getSandboxStatus());
        ClientApiSchema.Property publicApiProperty = clientApiObject.getProperties().stream()
                .filter(property -> property.getTitle().equals(PUBLIC_PROPERTY_NAME)).findFirst()
                .orElseThrow(() -> new BcException("Unable to load public property"));
        assertEquals(SandboxStatus.PUBLIC, publicApiProperty.getSandboxStatus());

        // ensure the sandboxed property appears on all the proper components
        assertEquals(2, publicApiConcept.getProperties().size());
        assertTrue(publicApiConcept.getProperties().stream().anyMatch(p -> p.equals(PUBLIC_PROPERTY_NAME)));
        assertTrue(publicApiConcept.getProperties().stream().anyMatch(p -> p.equals(SANDBOX_PROPERTY_NAME)));
        assertEquals(2, publicApiRelationship.getProperties().size());
        assertTrue(publicApiRelationship.getProperties().stream().anyMatch(p -> p.equals(PUBLIC_PROPERTY_NAME)));
        assertTrue(publicApiRelationship.getProperties().stream().anyMatch(p -> p.equals(SANDBOX_PROPERTY_NAME)));
        assertEquals(2, sandboxApiConcept.getProperties().size());
        assertEquals(SANDBOX_PROPERTY_NAME, sandboxApiConcept.getProperties().get(1));
        assertEquals(1, sandboxApiRelationship.getProperties().size());
        assertEquals(SANDBOX_PROPERTY_NAME, sandboxApiRelationship.getProperties().get(0));
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testCreatingConceptsWithNoUserOrWorkspace() {
        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        getSchemaRepository().getOrCreateConcept(thing, PUBLIC_CONCEPT_NAME, PUBLIC_DISPLAY_NAME, null, PUBLIC);
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testCreatingPublicConceptsWithoutPublishPrivilege() {
        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        getSchemaRepository().getOrCreateConcept(thing, PUBLIC_CONCEPT_NAME, PUBLIC_DISPLAY_NAME, user, PUBLIC);
    }

    @Test
    public void testCreatingPublicConcepts() {
        setPrivileges(user, Sets.newHashSet(Privilege.ONTOLOGY_ADD, Privilege.ONTOLOGY_PUBLISH));

        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        getSchemaRepository().getOrCreateConcept(thing, PUBLIC_CONCEPT_NAME, PUBLIC_DISPLAY_NAME, user, PUBLIC);
        getSchemaRepository().clearCache();

        Concept noWorkspace = getSchemaRepository().getConceptByName(PUBLIC_CONCEPT_NAME, PUBLIC);
        assertEquals(PUBLIC_DISPLAY_NAME, noWorkspace.getDisplayName());

        Concept withWorkspace = getSchemaRepository().getConceptByName(PUBLIC_CONCEPT_NAME, workspaceId);
        assertEquals(PUBLIC_DISPLAY_NAME, withWorkspace.getDisplayName());
    }

    @Test
    public void testCreatingPublicConceptsAsSystem() {
        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        getSchemaRepository().getOrCreateConcept(thing, PUBLIC_CONCEPT_NAME, PUBLIC_DISPLAY_NAME, systemUser, PUBLIC);
        getSchemaRepository().clearCache();

        Concept noWorkspace = getSchemaRepository().getConceptByName(PUBLIC_CONCEPT_NAME, PUBLIC);
        assertEquals(PUBLIC_DISPLAY_NAME, noWorkspace.getDisplayName());

        Concept withWorkspace = getSchemaRepository().getConceptByName(PUBLIC_CONCEPT_NAME, workspaceId);
        assertEquals(PUBLIC_DISPLAY_NAME, withWorkspace.getDisplayName());
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testCreatingSandboxedConceptsWithoutAddPermissionPrivilege() {
        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        getSchemaRepository().getOrCreateConcept(thing, SANDBOX_CONCEPT_NAME, SANDBOX_DISPLAY_NAME, user, workspaceId);
    }

    @Test
    public void testCreatingSandboxedConcepts() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        getSchemaRepository().getOrCreateConcept(thing, SANDBOX_CONCEPT_NAME, SANDBOX_DISPLAY_NAME, user, workspaceId);
        getSchemaRepository().clearCache();

        Concept noWorkspace = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, PUBLIC);
        assertNull(noWorkspace);

        Concept withWorkspace = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, workspaceId);
        assertEquals(SANDBOX_DISPLAY_NAME, withWorkspace.getDisplayName());
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testPublishingConceptsWithoutPublishPrivilege() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        Concept sandboxedConcept = getSchemaRepository().getOrCreateConcept(thing, SANDBOX_CONCEPT_NAME, SANDBOX_DISPLAY_NAME, user, workspaceId);
        getSchemaRepository().clearCache();

        getSchemaRepository().publishConcept(sandboxedConcept, user, workspaceId);
    }

    @Test
    public void testPublishingConcepts() {
        setPrivileges(user, Sets.newHashSet(Privilege.ONTOLOGY_ADD, Privilege.ONTOLOGY_PUBLISH));

        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        Concept sandboxedConcept = getSchemaRepository().getOrCreateConcept(thing, SANDBOX_CONCEPT_NAME, SANDBOX_DISPLAY_NAME, user, workspaceId);
        getSchemaRepository().publishConcept(sandboxedConcept, user, workspaceId);
        getSchemaRepository().clearCache();
        getGraph().flush();

        Concept publicConcept = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, PUBLIC);
        assertEquals(SANDBOX_DISPLAY_NAME, publicConcept.getDisplayName());
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testCreatingRelationshipsWithNoUserOrWorkspace() {
        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        getSchemaRepository().getOrCreateRelationshipType(null, thing, thing, PUBLIC_RELATIONSHIP_NAME, null, true, true, null, PUBLIC);
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testCreatingPublicRelationshipsWithoutPublishPrivilege() {
        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        getSchemaRepository().getOrCreateRelationshipType(null, thing, thing, PUBLIC_RELATIONSHIP_NAME, null, true, true, user, PUBLIC);
    }

    @Test
    public void testCreatingPublicRelationships() {
        setPrivileges(user, Sets.newHashSet(Privilege.ONTOLOGY_ADD, Privilege.ONTOLOGY_PUBLISH));

        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        getSchemaRepository().getOrCreateRelationshipType(null, thing, thing, PUBLIC_RELATIONSHIP_NAME, null, true, true, user, PUBLIC);
        getSchemaRepository().clearCache();

        Relationship noWorkspace = getSchemaRepository().getRelationshipByName(PUBLIC_RELATIONSHIP_NAME, PUBLIC);
        assertEquals(PUBLIC_RELATIONSHIP_NAME, noWorkspace.getName());

        Relationship withWorkspace = getSchemaRepository().getRelationshipByName(PUBLIC_RELATIONSHIP_NAME, workspaceId);
        assertEquals(PUBLIC_RELATIONSHIP_NAME, withWorkspace.getName());
    }

    @Test
    public void testCreatingPublicRelationshipsAsSystem() {
        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        getSchemaRepository().getOrCreateRelationshipType(null, thing, thing, PUBLIC_RELATIONSHIP_NAME, null, true, true, systemUser, PUBLIC);
        getSchemaRepository().clearCache();

        Relationship noWorkspace = getSchemaRepository().getRelationshipByName(PUBLIC_RELATIONSHIP_NAME, PUBLIC);
        assertEquals(PUBLIC_RELATIONSHIP_NAME, noWorkspace.getName());

        Relationship withWorkspace = getSchemaRepository().getRelationshipByName(PUBLIC_RELATIONSHIP_NAME, workspaceId);
        assertEquals(PUBLIC_RELATIONSHIP_NAME, withWorkspace.getName());
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testCreatingSandboxedRelationshipsWithoutAddPermissionPrivilege() {
        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        getSchemaRepository().getOrCreateRelationshipType(null, thing, thing, SANDBOX_RELATIONSHIP_NAME, null, true, true, user, workspaceId);
    }

    @Test
    public void testCreatingSandboxedRelationships() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        getSchemaRepository().getOrCreateRelationshipType(null, thing, thing, SANDBOX_RELATIONSHIP_NAME, null, true, true, user, workspaceId);
        getSchemaRepository().clearCache();

        Relationship noWorkspace = getSchemaRepository().getRelationshipByName(SANDBOX_RELATIONSHIP_NAME, PUBLIC);
        assertNull(noWorkspace);

        Relationship withWorkspace = getSchemaRepository().getRelationshipByName(SANDBOX_RELATIONSHIP_NAME, workspaceId);
        assertEquals(SANDBOX_RELATIONSHIP_NAME, withWorkspace.getName());
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testPublishingRelationshipsWithoutPublishPrivilege() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        Relationship sandboxedRelationship = getSchemaRepository().getOrCreateRelationshipType(null, thing, thing, SANDBOX_RELATIONSHIP_NAME, null, true, true, user, workspaceId);
        getSchemaRepository().publishRelationship(sandboxedRelationship, user, workspaceId);
    }

    @Test
    public void testPublishingRelationships() {
        setPrivileges(user, Sets.newHashSet(Privilege.ONTOLOGY_ADD, Privilege.ONTOLOGY_PUBLISH));

        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        Relationship sandboxedRelationship = getSchemaRepository().getOrCreateRelationshipType(null, thing, thing, SANDBOX_RELATIONSHIP_NAME, null, true, true, user, workspaceId);
        getSchemaRepository().publishRelationship(sandboxedRelationship, user, workspaceId);
        getSchemaRepository().clearCache();

        Relationship publicRelationship = getSchemaRepository().getRelationshipByName(SANDBOX_RELATIONSHIP_NAME, PUBLIC);
        assertEquals(SANDBOX_RELATIONSHIP_NAME, publicRelationship.getName());
    }

    @Test
    public void testAddingPublicConceptsToPublicRelationships() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        getWorkspaceRepository().add(workspaceId, "Junit Workspace", user);

        Concept publicConcept = createConcept(PUBLIC_CONCEPT_NAME, PUBLIC_DISPLAY_NAME, PUBLIC);
        Concept publicConceptB = createConcept(PUBLIC_CONCEPT_NAME + 'b', PUBLIC_DISPLAY_NAME, PUBLIC);
        createRelationship(PUBLIC_RELATIONSHIP_NAME, PUBLIC);

        getSchemaRepository().clearCache();

        try {
            getSchemaRepository().addDomainConceptsToRelationshipType(PUBLIC_RELATIONSHIP_NAME, Collections.singletonList(publicConcept.getName()), systemUser, workspaceId);
            fail();
        } catch (UnsupportedOperationException uoe) {
            // this shouldn't be supported yet
        }
        try {
            getSchemaRepository().addRangeConceptsToRelationshipType(PUBLIC_RELATIONSHIP_NAME, Collections.singletonList(publicConceptB.getName()), systemUser, workspaceId);
            fail();
        } catch (UnsupportedOperationException uoe) {
            // this shouldn't be supported yet
        }
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testCreatingPropertyWithNoUserOrWorkspace() {
        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        SchemaPropertyDefinition schemaPropertyDefinition = new SchemaPropertyDefinition(thing, PUBLIC_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, PropertyType.DATE);
        getSchemaRepository().getOrCreateProperty(schemaPropertyDefinition, null, PUBLIC);
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testCreatingPublicPropertyWithoutPublishPrivilege() {
        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        SchemaPropertyDefinition schemaPropertyDefinition = new SchemaPropertyDefinition(thing, PUBLIC_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, PropertyType.DATE);
        getSchemaRepository().getOrCreateProperty(schemaPropertyDefinition, user, PUBLIC);
    }

    @Test
    public void testCreatingPublicProperty() {
        setPrivileges(user, Sets.newHashSet(Privilege.ONTOLOGY_ADD, Privilege.ONTOLOGY_PUBLISH));

        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        SchemaPropertyDefinition schemaPropertyDefinition = new SchemaPropertyDefinition(thing, PUBLIC_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, PropertyType.DATE);
        getSchemaRepository().getOrCreateProperty(schemaPropertyDefinition, user, PUBLIC);
        getSchemaRepository().clearCache();

        SchemaProperty noWorkspace = getSchemaRepository().getPropertyByName(PUBLIC_PROPERTY_NAME, PUBLIC);
        assertEquals(PUBLIC_PROPERTY_NAME, noWorkspace.getName());

        SchemaProperty withWorkspace = getSchemaRepository().getPropertyByName(PUBLIC_PROPERTY_NAME, workspaceId);
        assertEquals(PUBLIC_PROPERTY_NAME, withWorkspace.getName());
    }

    @Test
    public void testCreatingPublicPropertyAsSystem() {
        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        SchemaPropertyDefinition schemaPropertyDefinition = new SchemaPropertyDefinition(thing, PUBLIC_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, PropertyType.DATE);
        getSchemaRepository().getOrCreateProperty(schemaPropertyDefinition, systemUser, PUBLIC);
        getSchemaRepository().clearCache();

        SchemaProperty noWorkspace = getSchemaRepository().getPropertyByName(PUBLIC_PROPERTY_NAME, PUBLIC);
        assertEquals(PUBLIC_PROPERTY_NAME, noWorkspace.getName());

        SchemaProperty withWorkspace = getSchemaRepository().getPropertyByName(PUBLIC_PROPERTY_NAME, workspaceId);
        assertEquals(PUBLIC_PROPERTY_NAME, withWorkspace.getName());
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testCreatingSandboxedPropertyWithoutAddPermissionPrivilege() {
        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        SchemaPropertyDefinition schemaPropertyDefinition = new SchemaPropertyDefinition(thing, SANDBOX_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, PropertyType.DATE);
        getSchemaRepository().getOrCreateProperty(schemaPropertyDefinition, user, workspaceId);
    }

    @Test
    public void testCreatingSandboxedProperty() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        List<Concept> things = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        Relationship publicRelationship = getSchemaRepository().getOrCreateRelationshipType(null, things, things, PUBLIC_RELATIONSHIP_NAME, true, true, systemUser, PUBLIC);
        SchemaPropertyDefinition schemaPropertyDefinition = new SchemaPropertyDefinition(
                things,
                Collections.singletonList(publicRelationship),
                SANDBOX_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, PropertyType.DATE);
        getSchemaRepository().getOrCreateProperty(schemaPropertyDefinition, user, workspaceId);
        getSchemaRepository().clearCache();

        SchemaProperty noWorkspace = getSchemaRepository().getPropertyByName(SANDBOX_PROPERTY_NAME, PUBLIC);
        assertNull(noWorkspace);

        Concept thing = getSchemaRepository().getThingConcept(PUBLIC);
        publicRelationship = getSchemaRepository().getRelationshipByName(PUBLIC_RELATIONSHIP_NAME, PUBLIC);
        int initialThingProperties = thing.getProperties().size();
        assertEquals(0, publicRelationship.getProperties().size());

        SchemaProperty withWorkspace = getSchemaRepository().getPropertyByName(SANDBOX_PROPERTY_NAME, workspaceId);
        assertEquals(SANDBOX_PROPERTY_NAME, withWorkspace.getName());

        thing = getSchemaRepository().getThingConcept(workspaceId);
        publicRelationship = getSchemaRepository().getRelationshipByName(PUBLIC_RELATIONSHIP_NAME, workspaceId);
        assertEquals(initialThingProperties + 1, thing.getProperties().size());
        assertThat(thing.getProperties().stream().map(SchemaProperty::getName).collect(Collectors.toSet()), org.hamcrest.Matchers.hasItem(SANDBOX_PROPERTY_NAME));
        assertEquals(1, publicRelationship.getProperties().size());
        assertEquals(SANDBOX_PROPERTY_NAME, publicRelationship.getProperties().iterator().next().getName());
    }

    @Test
    public void testAddingPublicPropertyToPublicConceptsAndRelationships() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        getWorkspaceRepository().add(workspaceId, "Junit Workspace", user);

        createConcept(PUBLIC_CONCEPT_NAME, PUBLIC_DISPLAY_NAME, PUBLIC);
        createRelationship(PUBLIC_RELATIONSHIP_NAME, PUBLIC);
        SchemaProperty publicProperty = createProperty(PUBLIC_PROPERTY_NAME, PUBLIC_DISPLAY_NAME, PUBLIC);

        getSchemaRepository().clearCache();

        try {
            getSchemaRepository().updatePropertyDomainNames(publicProperty, Sets.newHashSet(PUBLIC_CONCEPT_NAME, PUBLIC_RELATIONSHIP_NAME), systemUser, workspaceId);
            fail();
        } catch (UnsupportedOperationException uoe) {
            // this shouldn't be supported yet
        }
    }

    @Test
    public void testAddingSandboxedPropertyToPublicConceptsAndRelationships() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        getWorkspaceRepository().add(workspaceId, "Junit Workspace", user);

        createConcept(PUBLIC_CONCEPT_NAME, PUBLIC_DISPLAY_NAME, PUBLIC);
        createRelationship(PUBLIC_RELATIONSHIP_NAME, PUBLIC);
        SchemaProperty sandboxedProperty = createProperty(SANDBOX_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, workspaceId);

        getSchemaRepository().clearCache();

        getSchemaRepository().updatePropertyDomainNames(sandboxedProperty, Sets.newHashSet(PUBLIC_CONCEPT_NAME, PUBLIC_RELATIONSHIP_NAME), systemUser, workspaceId);

        getSchemaRepository().clearCache();

        // ensure that it's there in the sandbox
        Concept publicConcept = getSchemaRepository().getConceptByName(PUBLIC_CONCEPT_NAME, workspaceId);
        assertEquals(1, publicConcept.getProperties().size());
        assertEquals(SANDBOX_PROPERTY_NAME, publicConcept.getProperties().iterator().next().getName());

        Relationship publicRelationship = getSchemaRepository().getRelationshipByName(PUBLIC_RELATIONSHIP_NAME, workspaceId);
        assertEquals(1, publicRelationship.getProperties().size());
        assertEquals(SANDBOX_PROPERTY_NAME, publicRelationship.getProperties().iterator().next().getName());

        // ensure that it's not there outside the sandbox
        publicConcept = getSchemaRepository().getConceptByName(PUBLIC_CONCEPT_NAME, PUBLIC);
        assertEquals(0, publicConcept.getProperties().size());

        publicRelationship = getSchemaRepository().getRelationshipByName(PUBLIC_RELATIONSHIP_NAME, PUBLIC);
        assertEquals(0, publicRelationship.getProperties().size());
    }

    @Test
    public void testAddingPublicPropertyToSandboxedConceptsAndRelationships() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        getWorkspaceRepository().add(workspaceId, "Junit Workspace", user);

        createConcept(SANDBOX_CONCEPT_NAME, SANDBOX_DISPLAY_NAME, workspaceId);
        createRelationship(SANDBOX_RELATIONSHIP_NAME, workspaceId);
        SchemaProperty publicProperty = createProperty(PUBLIC_PROPERTY_NAME, PUBLIC_DISPLAY_NAME, PUBLIC);

        getSchemaRepository().clearCache();

        try {
            getSchemaRepository().updatePropertyDomainNames(publicProperty, Sets.newHashSet(SANDBOX_CONCEPT_NAME, SANDBOX_RELATIONSHIP_NAME), systemUser, workspaceId);
            fail();
        } catch (UnsupportedOperationException uoe) {
            // this shouldn't be supported yet
        }
    }

    @Test
    public void testAddingSandboxedPropertyToSandboxedConceptsAndRelationships() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        getWorkspaceRepository().add(workspaceId, "Junit Workspace", user);

        createConcept(SANDBOX_CONCEPT_NAME, SANDBOX_DISPLAY_NAME, workspaceId);
        createRelationship(SANDBOX_RELATIONSHIP_NAME, workspaceId);
        SchemaProperty sandboxedProperty = createProperty(SANDBOX_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, workspaceId);

        getSchemaRepository().clearCache();

        getSchemaRepository().updatePropertyDomainNames(sandboxedProperty, Sets.newHashSet(SANDBOX_CONCEPT_NAME, SANDBOX_RELATIONSHIP_NAME), systemUser, workspaceId);

        getSchemaRepository().clearCache();

        Concept sandboxedConcept = getSchemaRepository().getConceptByName(SANDBOX_CONCEPT_NAME, workspaceId);
        assertEquals(1, sandboxedConcept.getProperties().size());
        assertEquals(SANDBOX_PROPERTY_NAME, sandboxedConcept.getProperties().iterator().next().getName());

        Relationship sandboxedRelationship = getSchemaRepository().getRelationshipByName(SANDBOX_RELATIONSHIP_NAME, workspaceId);
        assertEquals(1, sandboxedRelationship.getProperties().size());
        assertEquals(SANDBOX_PROPERTY_NAME, sandboxedRelationship.getProperties().iterator().next().getName());
    }

    @Test(expected = BcAccessDeniedException.class)
    public void testPublishingPropertyWithoutPublishPrivilege() {
        setPrivileges(user, Collections.singleton(Privilege.ONTOLOGY_ADD));

        List<Concept> thing = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        SchemaPropertyDefinition schemaPropertyDefinition = new SchemaPropertyDefinition(thing, SANDBOX_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, PropertyType.DATE);
        SchemaProperty sandboxedProperty = getSchemaRepository().getOrCreateProperty(schemaPropertyDefinition, user, workspaceId);
        getSchemaRepository().publishProperty(sandboxedProperty, user, workspaceId);
    }

    @Test
    public void testPublishingProperty() {
        setPrivileges(user, Sets.newHashSet(Privilege.ONTOLOGY_ADD, Privilege.ONTOLOGY_PUBLISH));

        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        int numThingProperties = thing.getProperties().size();
        List<Concept> things = Collections.singletonList(thing);
        Relationship publicRelationship = getSchemaRepository().getOrCreateRelationshipType(null, things, things, PUBLIC_RELATIONSHIP_NAME, true, true, systemUser, PUBLIC);
        SchemaPropertyDefinition schemaPropertyDefinition = new SchemaPropertyDefinition(
                things,
                Collections.singletonList(publicRelationship),
                SANDBOX_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, PropertyType.DATE);
        SchemaProperty sandboxedProperty = getSchemaRepository().getOrCreateProperty(schemaPropertyDefinition, user, workspaceId);
        getSchemaRepository().publishProperty(sandboxedProperty, user, workspaceId);
        getSchemaRepository().clearCache();

        SchemaProperty publicProperty = getSchemaRepository().getPropertyByName(SANDBOX_PROPERTY_NAME, PUBLIC);
        assertEquals(SANDBOX_PROPERTY_NAME, publicProperty.getName());

        thing = getSchemaRepository().getThingConcept(PUBLIC);
        publicRelationship = getSchemaRepository().getRelationshipByName(PUBLIC_RELATIONSHIP_NAME, PUBLIC);
        assertEquals(numThingProperties+1, thing.getProperties().size());
        assertThat(thing.getProperties().stream().map(SchemaProperty::getName).collect(Collectors.toSet()), org.hamcrest.Matchers.hasItem(SANDBOX_PROPERTY_NAME));
        assertEquals(1, publicRelationship.getProperties().size());
        assertEquals(SANDBOX_PROPERTY_NAME, publicRelationship.getProperties().iterator().next().getName());
    }

    @Test
    public void testProperlyConfiguredThingConcept() throws Exception {
        createSampleOntology();

        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        assertNotNull(thing.getTitleFormula());
        assertNotNull(thing.getSubtitleFormula());
        assertNotNull(thing.getTimeFormula());
    }

    private SampleSchemaDetails createSampleOntology() {
        setPrivileges(user, Sets.newHashSet(Privilege.ONTOLOGY_ADD, Privilege.ONTOLOGY_PUBLISH));

        getWorkspaceRepository().add(workspaceId, "Junit Workspace", user);

        Concept publicConcept = createConcept(PUBLIC_CONCEPT_NAME, PUBLIC_DISPLAY_NAME, PUBLIC);
        Concept sandboxedConcept = createConcept(SANDBOX_CONCEPT_NAME, SANDBOX_DISPLAY_NAME, workspaceId);

        Relationship publicRelationship = createRelationship(PUBLIC_RELATIONSHIP_NAME, PUBLIC);
        Relationship sandboxedRelationship = createRelationship(SANDBOX_RELATIONSHIP_NAME, workspaceId);

        SchemaProperty publicProperty = createProperty(PUBLIC_PROPERTY_NAME, PUBLIC_DISPLAY_NAME, publicConcept, publicRelationship, PUBLIC);
        SchemaProperty sandboxedProperty = createProperty(SANDBOX_PROPERTY_NAME, SANDBOX_DISPLAY_NAME, Arrays.asList(publicConcept, sandboxedConcept), Arrays.asList(publicRelationship, sandboxedRelationship), workspaceId);

        SchemaProperty sandboxedPropertyOnlySandboxedConcept = createProperty(SANDBOX_PROPERTY_NAME_ONLY_SANDBOXED_CONCEPT, SANDBOX_DISPLAY_NAME, Arrays.asList(sandboxedConcept), Arrays.asList(), workspaceId);

        getSchemaRepository().clearCache();

        return new SampleSchemaDetails(
                publicConcept.getId(), publicRelationship.getId(), publicProperty.getId(),
                sandboxedConcept.getId(),
                sandboxedRelationship.getId(), sandboxedProperty.getId(), sandboxedPropertyOnlySandboxedConcept.getId());
    }

    private Concept createConcept(String name, String displayName, String workspaceId) {
        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        return getSchemaRepository().getOrCreateConcept(thing, name, displayName, systemUser, workspaceId);
    }

    private Relationship createRelationship(String name, String workspaceId) {
        List<Concept> things = Collections.singletonList(getSchemaRepository().getThingConcept(workspaceId));
        return getSchemaRepository().getOrCreateRelationshipType(null, things, things, name, true, true, systemUser, workspaceId);
    }

    private SchemaProperty createProperty(String name, String displayName, String workspaceId) {
        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        return createProperty(name, displayName, Collections.singletonList(thing), Collections.emptyList(), workspaceId);
    }

    private SchemaProperty createProperty(String name, String displayName, Concept concept, Relationship relationship, String workspaceId) {
        return createProperty(name, displayName, Collections.singletonList(concept), Collections.singletonList(relationship), workspaceId);
    }

    private SchemaProperty createProperty(String name, String displayName, List<Concept> concepts, List<Relationship> relationships, String workspaceId) {
        SchemaPropertyDefinition publicPropertyDefinition = new SchemaPropertyDefinition(concepts, relationships, name, displayName, PropertyType.STRING);
        publicPropertyDefinition.setTextIndexHints(Collections.singleton(TextIndexHint.EXACT_MATCH));
        publicPropertyDefinition.setUserVisible(true);
        return getSchemaRepository().getOrCreateProperty(publicPropertyDefinition, systemUser, workspaceId);
    }

    private class SampleSchemaDetails {
        String publicConceptId;
        String publicRelationshipId;
        String publicPropertyId;

        String sandboxConceptId;
        String sandboxRelationshipId;
        String sandboxPropertyId;
        String sandboxPropertyIdSandboxedConcept;

        SampleSchemaDetails(String publicConceptId, String publicRelationshipId, String publicPropertyId, String sandboxConceptId, String sandboxRelationshipId, String sandboxPropertyId, String sandboxPropertyIdSandboxedConcept) {
            this.publicConceptId = publicConceptId;
            this.publicRelationshipId = publicRelationshipId;
            this.publicPropertyId = publicPropertyId;
            this.sandboxConceptId = sandboxConceptId;
            this.sandboxRelationshipId = sandboxRelationshipId;
            this.sandboxPropertyId = sandboxPropertyId;
            this.sandboxPropertyIdSandboxedConcept = sandboxPropertyIdSandboxedConcept;
        }
    }

    private void validateTestRelationship() {
        Relationship relationship = getSchemaRepository().getRelationshipByName("test#personKnowsPerson", PUBLIC);
        assertEquals("Knows", relationship.getDisplayName());
        assertEquals("prop('test#firstMet') || ''", relationship.getTimeFormula());
        assertTrue(relationship.getTargetConceptNames().contains("test#person"));
        assertTrue(relationship.getSourceConceptNames().contains("test#person"));

        relationship = getSchemaRepository().getRelationshipByName("test#personIsRelatedToPerson", PUBLIC);
        assertEquals("Is Related To", relationship.getDisplayName());
        String[] intents = relationship.getIntents();
        assertEquals(1, intents.length);
        assertEquals("test", intents[0]);
        assertTrue(relationship.getTargetConceptNames().contains("test#person"));
        assertTrue(relationship.getSourceConceptNames().contains("test#person"));
    }

    private void validateTestProperties() {
        SchemaProperty nameProperty = getSchemaRepository().getPropertyByName("test#name", PUBLIC);
        assertEquals("Name", nameProperty.getDisplayName());
        assertEquals(PropertyType.STRING, nameProperty.getDataType());
        assertEquals("__.compact([ dependentProp('test#firstName'), dependentProp('test#middleName'), dependentProp('test#lastName') ]).join(', ')", nameProperty.getDisplayFormula().trim());
        ImmutableList<String> dependentPropertyNames = nameProperty.getDependentPropertyNames();
        assertEquals(3, dependentPropertyNames.size());
        assertTrue(dependentPropertyNames.contains("test#firstName"));
        assertTrue(dependentPropertyNames.contains("test#middleName"));
        assertTrue(dependentPropertyNames.contains("test#lastName"));
        List<String> intents = Arrays.asList(nameProperty.getIntents());
        assertEquals(1, intents.size());
        assertTrue(intents.contains("test3"));
        assertEquals(
                "dependentProp('test#lastName') && dependentProp('test#firstName')",
                nameProperty.getValidationFormula()
        );
        assertEquals("Personal Information", nameProperty.getPropertyGroup());
        assertEquals("test", nameProperty.getDisplayType());
        assertFalse(nameProperty.getAddable());
        assertFalse(nameProperty.getUpdateable());
        assertFalse(nameProperty.getDeleteable());
        Map<String, String> possibleValues = nameProperty.getPossibleValues();
        assertEquals(2, possibleValues.size());
        assertEquals("test 1", possibleValues.get("T1"));
        assertEquals("test 2", possibleValues.get("T2"));

        Concept person = getSchemaRepository().getConceptByName("test#person", PUBLIC);
        assertTrue(person.getProperties()
                .stream()
                .anyMatch(p -> p.getName().equals(nameProperty.getName()))
        );

        SchemaProperty firstMetProperty = getSchemaRepository().getPropertyByName("test#firstMet", PUBLIC);
        assertEquals("First Met", firstMetProperty.getDisplayName());
        assertEquals(PropertyType.DATE, firstMetProperty.getDataType());

        SchemaProperty favColorProperty = getSchemaRepository().getPropertyByName("test#favoriteColor", PUBLIC);
        assertEquals("Favorite Color", favColorProperty.getDisplayName());
        possibleValues = favColorProperty.getPossibleValues();
        assertEquals(2, possibleValues.size());
        assertEquals("red 1", possibleValues.get("Red"));
        assertEquals("blue 2", possibleValues.get("Blue"));

        Relationship relationship = getSchemaRepository().getRelationshipByName("test#personKnowsPerson", PUBLIC);
        assertTrue(relationship.getProperties()
                .stream()
                .anyMatch(p -> p.getName().equals(firstMetProperty.getName()))
        );
    }

    private void validateTestExtendedDataTables() {
        SchemaProperty personTable = getSchemaRepository().getPropertyByName("test#personExtendedDataTable", PUBLIC);
        assertTrue("personTable should be an instance of " + ExtendedDataTableProperty.class.getName(), personTable instanceof ExtendedDataTableProperty);
        ExtendedDataTableProperty edtp = (ExtendedDataTableProperty) personTable;
        ImmutableList<String> columns = edtp.getTablePropertyNames();
        assertEquals(2, columns.size());
    }

    private void validateTestConcepts(int expectedNameSize) throws IOException {
        Concept contact = getSchemaRepository().getConceptByName("test#contact", PUBLIC);
        assertEquals("Contact", contact.getDisplayName());
        assertEquals("rgb(149, 138, 218)", contact.getColor());
        assertEquals("test", contact.getDisplayType());
        List<String> intents = Arrays.asList(contact.getIntents());
        assertEquals(1, intents.size());
        assertTrue(intents.contains("face"));

        Concept person = getSchemaRepository().getConceptByName("test#person", PUBLIC);
        assertEquals("Person", person.getDisplayName());
        intents = Arrays.asList(person.getIntents());
        assertEquals(1, intents.size());
        assertTrue(intents.contains("person"));
        assertEquals("prop('test#birthDate') || ''", person.getTimeFormula());
        assertEquals("prop('test#name') || ''", person.getTitleFormula());

        InputStream icon = SchemaRepositoryTestBase.class.getResourceAsStream(GLYPH_ICON_FILE);
        byte[] bytes = IOUtils.toByteArray(icon);
        assertArrayEquals(bytes, person.getGlyphIcon());
        assertEquals("rgb(28, 137, 28)", person.getColor());

        Set<Concept> conceptAndAllChildren = getSchemaRepository().getConceptAndAllChildren(contact, PUBLIC);
        List<String> names = Lists.newArrayList();
        conceptAndAllChildren.forEach(c -> names.add(c.getName()));
        assertEquals(expectedNameSize, names.size());
        assertTrue(names.contains(contact.getName()));
        assertTrue(names.contains(person.getName()));
    }

    private void validateChangedOwlRelationships() throws IOException {
        Relationship relationship = getSchemaRepository().getRelationshipByName("test#personKnowsPerson", PUBLIC);
        assertEquals("Person Knows Person", relationship.getDisplayName());
        assertNull(relationship.getTimeFormula());
        assertTrue(relationship.getTargetConceptNames().contains("test#person"));
        assertTrue(relationship.getSourceConceptNames().contains("test#person"));

        Concept thing = getSchemaRepository().getThingConcept(workspaceId);
        assertNotNull(thing.getTitleFormula());
        assertNotNull(thing.getSubtitleFormula());
        assertNotNull(thing.getTimeFormula());
    }

    private void validateChangedOwlConcepts() throws IOException {
        Concept contact = getSchemaRepository().getConceptByName("test#contact", PUBLIC);
        Concept person = getSchemaRepository().getConceptByName("test#person", PUBLIC);
        assertEquals("Person", person.getDisplayName());
        List<String> intents = Arrays.asList(person.getIntents());
        assertEquals(1, intents.size());
        assertFalse(intents.contains("person"));
        assertFalse(intents.contains("face"));
        assertTrue(intents.contains("test"));
        assertNull(person.getTimeFormula());
        assertEquals("prop('test#name') || ''", person.getTitleFormula());

        InputStream icon = SchemaRepositoryBase.class.getResourceAsStream(RESOURCE_ENTITY_PNG);
        byte[] bytes = IOUtils.toByteArray(icon);
        assertArrayEquals(bytes, person.getGlyphIcon());

        assertEquals("rgb(28, 137, 28)", person.getColor());

        Set<Concept> conceptAndAllChildren = getSchemaRepository().getConceptAndAllChildren(contact, PUBLIC);
        List<String> names = Lists.newArrayList();
        conceptAndAllChildren.forEach(c -> names.add(c.getName()));
        assertEquals(2, names.size());
        assertTrue(names.contains(contact.getName()));
        assertTrue(names.contains(person.getName()));
    }

    private void validateChangedOwlProperties() throws IOException {
        SchemaProperty nameProperty = getSchemaRepository().getPropertyByName("test#name", PUBLIC);
        assertEquals("Name", nameProperty.getDisplayName());
        assertEquals(PropertyType.STRING, nameProperty.getDataType());
        assertEquals("_.compact([ dependentProp('test#firstName'), dependentProp('test#lastName')]).join(', ')", nameProperty.getDisplayFormula().trim());
        ImmutableList<String> dependentPropertyNames = nameProperty.getDependentPropertyNames();
        assertEquals(3, dependentPropertyNames.size());
        assertTrue(dependentPropertyNames.contains("test#firstName"));
        assertTrue(dependentPropertyNames.contains("test#middleName"));
        assertTrue(dependentPropertyNames.contains("test#lastName"));
        List<String> intents = Arrays.asList(nameProperty.getIntents());
        assertEquals(1, intents.size());
        assertTrue(intents.contains("test3"));
        assertEquals(
                "dependentProp('test#lastName') && dependentProp('test#firstName')",
                nameProperty.getValidationFormula()
        );
        assertEquals("Personal Information", nameProperty.getPropertyGroup());
        assertEquals("test 2", nameProperty.getDisplayType());
        assertTrue(nameProperty.getAddable());
        assertTrue(nameProperty.getUpdateable());
        assertTrue(nameProperty.getDeleteable());
        Map<String, String> possibleValues = nameProperty.getPossibleValues();
        assertNull(possibleValues);

        SchemaProperty firstMetProperty = getSchemaRepository().getPropertyByName( "test#firstMet", PUBLIC);
        assertEquals("First Met", firstMetProperty.getDisplayName());
        assertEquals(PropertyType.DATE, firstMetProperty.getDataType());

        SchemaProperty favColorProperty = getSchemaRepository().getPropertyByName("test#favoriteColor", PUBLIC);
        assertEquals("Favorite Color", favColorProperty.getDisplayName());
        possibleValues = favColorProperty.getPossibleValues();
        assertEquals(2, possibleValues.size());
        assertEquals("red 1", possibleValues.get("Red"));
        assertEquals("blue 2", possibleValues.get("Blue"));

        Relationship relationship = getSchemaRepository().getRelationshipByName("test#personKnowsPerson", PUBLIC);
        assertTrue(relationship.getProperties()
                .stream()
                .anyMatch(p -> p.getName().equals(firstMetProperty.getName()))
        );
    }

    @Test
    public void testLoadSchema() throws Exception {
        loadTestSchema();
    }

    private void loadTestSchema() throws Exception {
        createTestSchema();
        validateTestRelationship();
        validateTestConcepts(2);
        validateTestProperties();
        validateTestExtendedDataTables();
    }

    private void loadHierarchySchema() throws Exception {
        createTestHyerarchyOntology();
    }

    private void createTestSchema() {
        SchemaFactory factory = new SchemaFactory(getSchemaRepository())
                .forNamespace(PUBLIC);

        Concept contact = factory.newConcept()
                .parent(factory.getOrCreateThingConcept())
                .conceptType("test#contact")
                .property(SchemaProperties.COLOR.getPropertyName(), stringValue("rgb(149, 138, 218)"))
                .property(SchemaProperties.DISPLAY_TYPE.getPropertyName(), stringValue("test"))
                .intents("face")
                .displayName("Contact")
                .save();

        Concept person = factory.newConcept()
                .conceptType("test#person")
                .parent(contact)
                .property(SchemaProperties.COLOR.getPropertyName(), stringValue("rgb(28, 137, 28)"))
                .property(SchemaProperties.TIME_FORMULA.getPropertyName(), stringValue("prop('test#birthDate') || ''"))
                .property(SchemaProperties.TITLE_FORMULA.getPropertyName(), stringValue("prop('test#name') || ''"))
                .glyphIcon(SchemaRepositoryTestBase.class.getResourceAsStream(GLYPH_ICON_FILE))
                .intents("person")
                .displayName("Person")
                .save();

        Relationship personIsRelatedToPerson = factory.newRelationship()
                .parent(factory.getOrCreateRootRelationship())
                .label("test#personIsRelatedToPerson")
                .source(person)
                .target(person)
                .intents("test")
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Is Related To"))
                .save();

        Relationship personIsSisterOfPerson = factory.newRelationship()
                .label("test#personIsSisterOfPerson")
                .parent(personIsRelatedToPerson)
                .source(person)
                .target(person)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Is Sister Of"))
                .save();

        Relationship personKnowsPerson = factory.newRelationship()
                .label("test#personKnowsPerson")
                .parent(factory.getOrCreateRootRelationship())
                .source(person)
                .target(person)
                .property(SchemaProperties.TIME_FORMULA.getPropertyName(), stringValue("prop('test#firstMet') || ''"))
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Knows"))
                .save();

        factory.newConceptProperty()
                .concepts(person)
                .name("test#favoriteColor")
                .type(PropertyType.STRING)
                .possibleValues(new HashMap<String, String>() {{
                    put("Red", "red 1");
                    put("Blue", "blue 2");
                }})
                .textIndexHints(TextIndexHint.EXACT_MATCH)
                .displayName("Favorite Color")
                .save();

        factory.newConceptProperty()
                .forRelationships(personKnowsPerson)
                .name("test#firstMet")
                .type(PropertyType.DATE)
                .displayName("First Met")
                .save();

        factory.newConceptProperty()
                .concepts(person)
                .name("test#firstName")
                .type(PropertyType.STRING)
                .save();

        factory.newConceptProperty()
                .concepts(person)
                .name("test#middleName")
                .type(PropertyType.STRING)
                .save();

        factory.newConceptProperty()
                .concepts(person)
                .name("test#lastName")
                .type(PropertyType.STRING)
                .save();

        factory.newConceptProperty()
                .concepts(person)
                .name("test#name")
                .displayName("Name")
                .displayFormula("__.compact([ dependentProp('test#firstName'), dependentProp('test#middleName'), dependentProp('test#lastName') ]).join(', ')")
                .type(PropertyType.STRING)
                .addable(false)
                .deletable(false)
                .dependentPropertyNames(ImmutableList.of("test#firstName", "test#middleName", "test#lastName"))
                .displayType("test")
                .intents("test3")
                .possibleValues(new HashMap<String, String>() {{
                    put("T1", "test 1");
                    put("T2", "test 2");
                }})
                .propertyGroup("Personal Information")
                .textIndexHints(TextIndexHint.ALL)
                .updatable(false)
                .validationFormula("dependentProp('test#lastName') && dependentProp('test#firstName')")
                .save();

        ExtendedDataTableProperty personTable = (ExtendedDataTableProperty) factory.newConceptProperty()
                .name("test#personExtendedDataTable")
                .concepts(person)
                .type(PropertyType.EXTENDED_DATA_TABLE)
                .displayName("Person Table")
                .save();

        factory.newConceptProperty()
                .name("test#personExtendedDataTableColumn1")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.EXACT_MATCH)
                .displayName("Person Table Column 1")
                .extendedDataTableNames(ImmutableList.of("test#personExtendedDataTable"))
                .save();

        factory.newConceptProperty()
                .name("test#personExtendedDataTableColumn2")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.EXACT_MATCH)
                .displayName("Person Table Column 2")
                .extendedDataTableNames(ImmutableList.of("test#personExtendedDataTable"))
                .save();


        System.out.println("Created test ontology");
    }

    private void createTestChangedSchema() {
        SchemaFactory factory = new SchemaFactory(getSchemaRepository())
                .forNamespace(PUBLIC);

        Concept person = factory.newConcept()
                .conceptType("test#person")
                .intents("test")
                .property(SchemaProperties.COLOR.getPropertyName(), stringValue("rgb(28, 137, 28)"))
                .property(SchemaProperties.TITLE_FORMULA.getPropertyName(), stringValue("prop('test#name') || ''"))
                .save();

        factory.newRelationship()
                .label("test#personKnowsPerson")
                .source(person)
                .target(person)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Person Knows Person"))
                .save();

        factory.newConceptProperty()
                .concepts(person)
                .name("test#name")
                .textIndexHints(TextIndexHint.ALL)
                .type(PropertyType.INTEGER)
                .displayFormula("_.compact([ dependentProp('test#firstName'), dependentProp('test#lastName')]).join(', ')")
                .validationFormula("dependentProp('test#lastName') && dependentProp('test#firstName')")
                .displayType("test 2")
                .propertyGroup("Personal Information")
                .addable(true)
                .updatable(true)
                .deletable(true)
                .intents("test3")
                .save();
    }


    private void createTestHyerarchyOntology() {
        SchemaFactory factory = new SchemaFactory(getSchemaRepository())
                .forNamespace(PUBLIC);

        Concept contact = factory.newConcept()
                .parent(factory.getOrCreateThingConcept())
                .conceptType("testhierarchy#contact")
                .property(SchemaProperties.COLOR.getPropertyName(), stringValue("rgb(149, 138, 218)"))
                .property(SchemaProperties.DISPLAY_TYPE.getPropertyName(), stringValue("test"))
                .displayName("Contact")
                .save();

        Concept person = factory.newConcept()
                .conceptType("testhierarchy#person")
                .displayName("Person")
                .intents("person", "face")
                .property(SchemaProperties.TIME_FORMULA.getPropertyName(), stringValue("prop('testhierarchy#birthDate') || ''"))
                .property(SchemaProperties.TITLE_FORMULA.getPropertyName(), stringValue("prop('testhierarchy#name') || ''"))
                .glyphIcon(SchemaRepositoryTestBase.class.getResourceAsStream(GLYPH_ICON_FILE))
                .property(SchemaProperties.COLOR.getPropertyName(), stringValue("rgb(28, 137, 28)"))
                .parent(contact)
                .save();

        Relationship personKnowsPerson = factory.newRelationship()
                .parent(factory.getOrCreateRootRelationship())
                .label("testhierarchy#personKnowsPerson")
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Knows"))
                .property(SchemaProperties.TIME_FORMULA.getPropertyName(), stringValue("prop('testhierarchy#firstMet') || ''"))
                .source(person)
                .target(person)
                .save();

        Relationship personReallyKnowsPerson = factory.newRelationship()
                .parent(personKnowsPerson)
                .label("testhierarchy#personReallyKnowsPerson")
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Really Knows"))
                .property(SchemaProperties.TIME_FORMULA.getPropertyName(), stringValue("prop('testhierarchy#firstMet') || ''"))
                .source(person)
                .target(person)
                .save();

        factory.newConceptProperty()
                .concepts(person)
                .name("testhierarchy#firstName")
                .type(PropertyType.STRING)
                .save();

        factory.newConceptProperty()
                .concepts(person)
                .name("testhierarchy#middleName")
                .type(PropertyType.STRING)
                .save();

        factory.newConceptProperty()
                .concepts(person)
                .name("testhierarchy#lastName")
                .type(PropertyType.STRING)
                .save();

        factory.newConceptProperty()
                .name("testhierarchy#name")
                .displayName("Name")
                .textIndexHints(TextIndexHint.ALL)
                .concepts(person)
                .type(PropertyType.STRING)
                .displayFormula("__.compact([ dependentProp('testhierarchy#firstName'), dependentProp('testhierarchy#middleName'), dependentProp('testhierarchy#lastName') ]).join(', ')")
                .dependentPropertyNames(ImmutableList.of("testhierarchy#firstName", "testhierarchy#middleName", "testhierarchy#lastName"))
                .intents("test3")
                .validationFormula("dependentProp('testhierarchy#lastName') && dependentProp('testhierarchy#firstName')")
                .propertyGroup("Personal Information")
                .displayType("test")
                .addable(false)
                .updatable(false)
                .deletable(false)
                .possibleValues(new HashMap<String, String>() {{
                    put("T1", "test 1");
                    put("T2", "test 2");
                }})
                .save();

        factory.newConceptProperty()
                .name("testhierarchy#firstMet")
                .displayName("First Met")
                .forRelationships(personKnowsPerson)
                .type(PropertyType.DATE)
                .save();

        factory.newConceptProperty()
                .name("testhierarchy#contacted")
                .displayName("Contacted")
                .concepts(contact)
                .type(PropertyType.BOOLEAN)
                .save();
    }

    @Override
    protected SchemaRepository getSchemaRepository() {
        if (schemaRepository == null) {
            getCacheService().put("__cypherAcceptance", "simpleSchema", Boolean.TRUE, new CacheOptions());
        }
        return super.getSchemaRepository();
    }
}
