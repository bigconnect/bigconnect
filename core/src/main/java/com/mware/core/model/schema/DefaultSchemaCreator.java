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

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.*;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.user.UserPropertyPrivilegeRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.ping.PingSchema;
import com.mware.ge.TextIndexHint;
import com.mware.ge.values.storable.BooleanValue;
import com.mware.ge.values.storable.Values;

import java.util.EnumSet;
import java.util.HashMap;

import static com.mware.ge.values.storable.Values.stringValue;

public class DefaultSchemaCreator {
    private Concept thingConcept;
    private Relationship topObjectProperty;
    private Concept userConcept;
    private SchemaRepository schemaRepository;
    private boolean simple;
    private SchemaFactory schemaFactory;

    public DefaultSchemaCreator(SchemaRepository schemaRepository) {
        this(schemaRepository, false);
    }

    public DefaultSchemaCreator(SchemaRepository schemaRepository, boolean simple) {
        this.schemaRepository = schemaRepository;
        this.simple = simple;
        this.schemaFactory = new SchemaFactory(schemaRepository);
    }

    public void createOntology() {
        thingConcept = schemaFactory.getOrCreateThingConcept();
        topObjectProperty = schemaFactory.getOrCreateRootRelationship();

        createMetaOntology();
        createUserOntology();
        createRoleOntology();
        createWorkspaceOntology();

        if (simple) {
            return;
        }

        createBaseOntology();
        createSearchOntology();
        createLongRunningProcessOntology();
        createPingOntology();
        createRegexOntology();

        createRawObjectAndMediaOntology();
        createSpongeOntology();
        createNerOntology();
        createFaceRecognitionOntology();
    }

    public void createBaseOntology() {
        Relationship hasEntity = schemaFactory.newRelationship()
                .label(SchemaConstants.EDGE_LABEL_HAS_ENTITY)
                .parent(topObjectProperty)
                .source(thingConcept)
                .target(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Has Entity"))
                .intents(SchemaConstants.INTENT_ARTIFACT_HAS_ENTITY)
                .coreConcept(true)
                .save();

        Relationship hasDetectedEntity = schemaFactory.newRelationship()
                .label(SchemaConstants.EDGE_LABEL_HAS_DETECTED_ENTITY)
                .parent(topObjectProperty)
                .source(thingConcept)
                .target(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Has Detected Entity"))
                .coreConcept(true)
                .save();


        schemaFactory.newRelationship()
                .label(SchemaConstants.EDGE_LABEL_HAS_SOURCE)
                .parent(topObjectProperty)
                .source(thingConcept)
                .target(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Has Source"))
                .coreConcept(true)
                .inverseOf(hasEntity)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.VISIBILITY_JSON.getPropertyName())
                .displayName("Visibility JSON")
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.MODIFIED_BY.getPropertyName())
                .displayName("Modified By")
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.MODIFIED_DATE.getPropertyName())
                .sortable(true)
                .displayName("Modified Date")
                .type(PropertyType.DATE)
                .userVisible(true)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.DATA_WORKER_BLACK_LIST.getPropertyName())
                .displayName("Data Worker Black List")
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.DATA_WORKER_WHITE_LIST.getPropertyName())
                .displayName("Data Worker White List")
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.TERM_MENTION_FOR_ELEMENT_ID.getPropertyName())
                .displayName("TM For Element ID")
                .type(PropertyType.STRING)
                .save();

        Concept event = schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_EVENT)
                .displayName("Event")
                .parent(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .glyphIcon("event.png")
                .save();

        schemaFactory.newConceptProperty()
                .concepts(event)
                .name(BcSchema.EVENT_TIME.getPropertyName())
                .displayName("Event Time")
                .type(PropertyType.DATE)
                .sortable(true)
                .userVisible(true)
                .save();
    }

    public void createMetaOntology() {
        // meta-ontology
        // meta-ontology concepts
        Concept conceptConcept = schemaFactory.newConcept()
                .conceptType(SchemaRepository.TYPE_CONCEPT)
                .displayName("Ontology Concept")
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .coreConcept(true)
                .save();

        Concept propertyConcept = schemaFactory.newConcept()
                .conceptType(SchemaRepository.TYPE_PROPERTY)
                .displayName("Ontology Property")
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .coreConcept(true)
                .save();

        Concept relationshipConcept = schemaFactory.newConcept()
                .conceptType(SchemaRepository.TYPE_RELATIONSHIP)
                .displayName("Ontology Relationship")
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .coreConcept(true)
                .save();

        // meta-ontology relationships
        schemaFactory.newRelationship()
                .label(LabelName.IS_A.toString())
                .source(conceptConcept, relationshipConcept)
                .target(conceptConcept, relationshipConcept)
                .coreConcept(true)
                .save();

        schemaFactory.newRelationship()
                .label(LabelName.HAS_EDGE.toString())
                .source(conceptConcept)
                .target(relationshipConcept)
                .coreConcept(true)
                .save();

        schemaFactory.newRelationship()
                .label(LabelName.HAS_PROPERTY.toString())
                .source(conceptConcept, relationshipConcept)
                .target(propertyConcept)
                .coreConcept(true)
                .save();

        schemaFactory.newRelationship()
                .label(LabelName.INVERSE_OF.toString())
                .source(relationshipConcept)
                .target(relationshipConcept)
                .coreConcept(true)
                .save();
    }

    public void createUserOntology() {
        // concepts
        userConcept = schemaFactory.newConcept()
                .conceptType(UserRepository.USER_CONCEPT_NAME)
                .parent(thingConcept)
                .displayName("User")
                .coreConcept(true)
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .save();

        // properties
        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.AUTHORIZATIONS.getPropertyName())
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.CREATE_DATE.getPropertyName())
                .type(PropertyType.DATE)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.CURRENT_LOGIN_DATE.getPropertyName())
                .type(PropertyType.DATE)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.CURRENT_LOGIN_REMOTE_ADDR.getPropertyName())
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.CURRENT_WORKSPACE.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.DISPLAY_NAME.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.LOGIN_COUNT.getPropertyName())
                .type(PropertyType.INTEGER)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.PASSWORD_HASH.getPropertyName())
                .type(PropertyType.BINARY)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.PASSWORD_SALT.getPropertyName())
                .type(PropertyType.BINARY)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.PREVIOUS_LOGIN_DATE.getPropertyName())
                .type(PropertyType.DATE)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.PREVIOUS_LOGIN_REMOTE_ADDR.getPropertyName())
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserPropertyPrivilegeRepository.PRIVILEGES_PROPERTY_NAME)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.STATUS.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.UI_PREFERENCES.getPropertyName())
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.USERNAME.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.EMAIL_ADDRESS.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.PASSWORD_RESET_TOKEN.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(userConcept)
                .name(UserSchema.PASSWORD_RESET_TOKEN_EXPIRATION_DATE.getPropertyName())
                .type(PropertyType.DATE)
                .save();
    }

    public void createRoleOntology() {
        // concepts
        Concept roleConcept = schemaFactory.newConcept()
                .conceptType(AuthorizationRepository.ROLE_CONCEPT_NAME)
                .parent(thingConcept)
                .displayName("Role")
                .coreConcept(true)
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .save();

        // relations
        schemaFactory.newRelationship()
                .label(RoleSchema.ROLE_TO_USER_RELATIONSHIP_NAME)
                .parent(topObjectProperty)
                .source(roleConcept)
                .target(userConcept)
                .coreConcept(true)
                .save();

        // properties
        schemaFactory.newConceptProperty()
                .concepts(roleConcept)
                .name(RoleSchema.ROLE_NAME.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(roleConcept)
                .name(RoleSchema.DESCRIPTION.getPropertyName())
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(roleConcept)
                .name(RoleSchema.PRIVILEGES.getPropertyName())
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(roleConcept)
                .name(RoleSchema.GLOBAL.getPropertyName())
                .type(PropertyType.BOOLEAN)
                .save();
    }

    public void createWorkspaceOntology() {
        //concepts
        Concept workspace = schemaFactory.newConcept()
                .conceptType(WorkspaceSchema.WORKSPACE_CONCEPT_NAME)
                .parent(thingConcept)
                .displayName("Workspace")
                .coreConcept(true)
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .save();

        //relations
        schemaFactory.newRelationship()
                .label(WorkspaceSchema.WORKSPACE_TO_ENTITY_RELATIONSHIP_NAME)
                .parent(topObjectProperty)
                .source(workspace)
                .target(thingConcept)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("To Entity"))
                .coreConcept(true)
                .save();

        Relationship workspaceToUser = schemaFactory.newRelationship()
                .label(WorkspaceSchema.WORKSPACE_TO_USER_RELATIONSHIP_NAME)
                .parent(topObjectProperty)
                .source(workspace)
                .target(userConcept)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("To User"))
                .coreConcept(true)
                .save();

        schemaFactory.newRelationship()
                .label(WorkspaceSchema.WORKSPACE_TO_ONTOLOGY_RELATIONSHIP_NAME)
                .parent(topObjectProperty)
                .source(workspace)
                .target(thingConcept)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("To Ontology"))
                .coreConcept(true)
                .save();

        // properties
        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(WorkspaceSchema.WORKSPACE_TO_USER_ACCESS.getPropertyName())
                .type(PropertyType.STRING)
                .forRelationships(workspaceToUser)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(WorkspaceSchema.WORKSPACE_TO_USER_IS_CREATOR.getPropertyName())
                .type(PropertyType.BOOLEAN)
                .forRelationships(workspaceToUser)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(workspace)
                .name(WorkspaceSchema.TITLE.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();
    }

    public void createSearchOntology() {
        //concepts
        Concept savedSearch = schemaFactory.newConcept()
                .conceptType(SearchSchema.CONCEPT_TYPE_SAVED_SEARCH)
                .parent(thingConcept)
                .displayName("Saved search")
                .coreConcept(true)
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .save();

        // relations
        schemaFactory.newRelationship()
                .label(SearchSchema.HAS_SAVED_SEARCH)
                .parent(topObjectProperty)
                .source(userConcept)
                .target(savedSearch)
                .coreConcept(true)
                .save();

        // properties
        schemaFactory.newConceptProperty()
                .concepts(savedSearch)
                .name(SearchSchema.NAME.getPropertyName())
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(savedSearch)
                .name(SearchSchema.PARAMETERS.getPropertyName())
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(savedSearch)
                .name(SearchSchema.URL.getPropertyName())
                .type(PropertyType.STRING)
                .save();
    }

    public void createLongRunningProcessOntology() {
        //concepts
        Concept longRunningProcess = schemaFactory.newConcept()
                .conceptType(LongRunningProcessSchema.LONG_RUNNING_PROCESS_CONCEPT_NAME)
                .parent(thingConcept)
                .displayName("Long Running Process")
                .coreConcept(true)
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .save();

        // relations
        schemaFactory.newRelationship()
                .label(LongRunningProcessSchema.LONG_RUNNING_PROCESS_TO_USER_EDGE_NAME)
                .parent(topObjectProperty)
                .source(userConcept)
                .target(longRunningProcess)
                .coreConcept(true)
                .save();

        // properties
        schemaFactory.newConceptProperty()
                .concepts(longRunningProcess)
                .name(LongRunningProcessSchema.QUEUE_ITEM_JSON_PROPERTY.getPropertyName())
                .type(PropertyType.STRING)
                .searchable(false)
                .save();
    }

    public void createPingOntology() {
        //concepts
        Concept ping = schemaFactory.newConcept()
                .conceptType(PingSchema.CONCEPT_NAME_PING)
                .parent(thingConcept)
                .displayName("Ping")
                .coreConcept(true)
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .save();

        // properties
        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.CREATE_DATE.getPropertyName())
                .displayName("Create Date")
                .type(PropertyType.DATE)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.CREATE_REMOTE_ADDR.getPropertyName())
                .displayName("Create Remote Address")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.GRAPH_PROPERTY_WORKER_DATE.getPropertyName())
                .displayName("GPW Date")
                .type(PropertyType.DATE)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.GRAPH_PROPERTY_WORKER_HOST_ADDRESS.getPropertyName())
                .displayName("GPW Host Address")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.GRAPH_PROPERTY_WORKER_HOSTNAME.getPropertyName())
                .displayName("GPW Hostname")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.GRAPH_PROPERTY_WORKER_WAIT_TIME_MS.getPropertyName())
                .displayName("GPW Wait Time (ms)")
                .type(PropertyType.INTEGER)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.LONG_RUNNING_PROCESS_DATE.getPropertyName())
                .displayName("LRP Date")
                .type(PropertyType.DATE)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.LONG_RUNNING_PROCESS_HOST_ADDRESS.getPropertyName())
                .displayName("LRP Host Address")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.LONG_RUNNING_PROCESS_HOSTNAME.getPropertyName())
                .displayName("LRP Hostname")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.LONG_RUNNING_PROCESS_WAIT_TIME_MS.getPropertyName())
                .displayName("LRP Wait Time (ms)")
                .type(PropertyType.INTEGER)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.RETRIEVAL_TIME_MS.getPropertyName())
                .displayName("Retrieval Time (ms)")
                .type(PropertyType.INTEGER)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(ping)
                .name(PingSchema.SEARCH_TIME_MS.getPropertyName())
                .displayName("Search Time (ms)")
                .type(PropertyType.INTEGER)
                .save();
    }

    public void createRegexOntology() {
        //concepts
        Concept regex = schemaFactory.newConcept()
                .conceptType(RegexSchema.REGEX_CONCEPT_NAME)
                .parent(thingConcept)
                .displayName("Regex")
                .coreConcept(true)
                .property(SchemaProperties.UPDATEABLE.getPropertyName(), BooleanValue.FALSE)
                .save();

        // properties
        schemaFactory.newConceptProperty()
                .concepts(regex)
                .name(RegexSchema.REGEX_NAME.getPropertyName())
                .displayName("Regex Name")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(regex)
                .name(RegexSchema.REGEX_PATTERN.getPropertyName())
                .displayName("Regex pattern")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(regex)
                .name(RegexSchema.REGEX_CONCEPT.getPropertyName())
                .displayName("Regex concept")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();

    }

    private void createRawObjectAndMediaOntology() {
        // concepts
        Concept raw = schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_RAW)
                .displayName("Raw")
                .parent(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .save();

        Concept audio = schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_AUDIO)
                .displayName("Audio")
                .parent(raw)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_TYPE.getPropertyName(), stringValue(SchemaConstants.DISPLAY_TYPE_AUDIO))
                .intents("audio")
                .glyphIcon("audio.png")
                .save();

        Concept document = schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_DOCUMENT)
                .displayName("Document")
                .parent(raw)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_TYPE.getPropertyName(), stringValue(SchemaConstants.DISPLAY_TYPE_DOCUMENT))
                .intents("document")
                .glyphIcon("document.png")
                .save();

        Concept image = schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_IMAGE)
                .displayName("Image")
                .parent(raw)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_TYPE.getPropertyName(), stringValue(SchemaConstants.DISPLAY_TYPE_IMAGE))
                .intents("image", SchemaConstants.INTENT_ENTITY_IMAGE)
                .glyphIcon("image.png")
                .save();

        Concept video = schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_VIDEO)
                .displayName("Video")
                .parent(raw)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_TYPE.getPropertyName(), stringValue(SchemaConstants.DISPLAY_TYPE_VIDEO))
                .intents("video")
                .glyphIcon("video.png")
                .save();

        // relationships
        schemaFactory.newRelationship()
                .label(SchemaConstants.EDGE_LABEL_RAW_CONTAINS_IMAGE_OF_ENTITY)
                .parent(topObjectProperty)
                .source(image)
                .target(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Contains Image Of"))
                .intents(SchemaConstants.INTENT_ARTIFACT_CONTAINS_IMAGE, SchemaConstants.INTENT_ARTIFACT_CONTAINS_IMAGE_OF_ENTITY)
                .save();

        schemaFactory.newRelationship()
                .label(SchemaConstants.EDGE_LABEL_HAS_IMAGE)
                .parent(topObjectProperty)
                .source(thingConcept)
                .target(image)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Has Image"))
                .intents(SchemaConstants.INTENT_ENTITY_HAS_IMAGE)
                .save();

        // properties
        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.LIKES.getPropertyName())
                .displayName("Social Likes")
                .type(PropertyType.INTEGER)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .userVisible(true)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.SHARES.getPropertyName())
                .displayName("Social Shares")
                .type(PropertyType.INTEGER)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .userVisible(true)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.COMMENTS.getPropertyName())
                .displayName("Social Comments")
                .type(PropertyType.INTEGER)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .userVisible(true)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(MediaBcSchema.AUDIO_MP3.getPropertyName())
                .displayName("Audio MP3")
                .type(PropertyType.BINARY)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.CACHED_IMAGE.getPropertyName())
                .displayName("Cached Image")
                .type(PropertyType.BINARY)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.CONTENT_HASH.getPropertyName())
                .displayName("Content Hash")
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(SchemaProperties.DATA_TYPE.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.DETECTED_OBJECT.getPropertyName())
                .displayName("Detected Object")
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.ENTITY_IMAGE_URL.getPropertyName())
                .displayName("Entity Image URL")
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.ENTITY_IMAGE_VERTEX_ID.getPropertyName())
                .displayName("Entity Image Vertex ID")
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.FILE_NAME.getPropertyName())
                .displayName("File Name")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.SOURCE.getPropertyName())
                .displayName("Source")
                .type(PropertyType.STRING)
                .userVisible(true)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.URL.getPropertyName())
                .displayName("URL")
                .type(PropertyType.STRING)
                .displayType(SchemaConstants.CUSTOM_DISPLAY_LINK)
                .userVisible(true)
                .textIndexHints(TextIndexHint.ALL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.METADATA_JSON.getPropertyName())
                .displayName("Metadata JSON")
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.MIME_TYPE.getPropertyName())
                .displayName("Mime Type")
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();


        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(SchemaProperties.ONTOLOGY_TITLE.getPropertyName())
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.PROCESS.getPropertyName())
                .displayName("Process")
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.RAW.getPropertyName())
                .displayName("Raw Data")
                .type(PropertyType.BINARY)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(MediaBcSchema.RAW_POSTER_FRAME.getPropertyName())
                .displayName("Raw Poster Frame")
                .type(PropertyType.BINARY)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.ROW_KEY.getPropertyName())
                .displayName("Row Key")
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.TERM_MENTION.getPropertyName())
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.TEXT.getPropertyName())
                .displayName("Text")
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.FULL_TEXT))
                .userVisible(true)
                .displayType(SchemaConstants.CUSTOM_DISPLAY_LONGTEXT)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(BcSchema.TITLE.getPropertyName())
                .displayName("Title")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .userVisible(true)
                .intents(SchemaConstants.INTENT_ARTIFACT_TITLE)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.RAW_SENTIMENT.getPropertyName())
                .displayName("Sentiment")
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .userVisible(true)
                .searchFacet(true)
                .aggType("term")
                .possibleValues(new HashMap<String, String>() {{
                    put("positive", "Positive");
                    put("neutral", "Neutral");
                    put("negative", "Negative");
                }})
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.RAW_TYPE.getPropertyName())
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .displayName("Type")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(MediaBcSchema.VIDEO_FRAME.getPropertyName())
                .displayName("Video Frame")
                .type(PropertyType.BINARY)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(MediaBcSchema.VIDEO_PREVIEW_IMAGE.getPropertyName())
                .displayName("Video Preview Image")
                .type(PropertyType.BINARY)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(MediaBcSchema.VIDEO_TRANSCRIPT.getPropertyName())
                .displayName("Video Transcript")
                .type(PropertyType.STRING)
                .displayType(SchemaConstants.CUSTOM_DISPLAY_LONGTEXT)
                .textIndexHints(EnumSet.of(TextIndexHint.FULL_TEXT))
                .userVisible(true)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(video, audio)
                .name(MediaBcSchema.MEDIA_DURATION.getPropertyName())
                .displayName("Duration (seconds)")
                .type(PropertyType.STRING)
                .textIndexHints(TextIndexHint.ALL)
                .intents(SchemaConstants.INTENT_MEDIA_DURATION, SchemaConstants.INTENT_AUDIO_DURATION, SchemaConstants.INTENT_VIDEO_DURATION)
                .userVisible(true)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.GEOLOCATION_PROPERTY.getPropertyName())
                .displayName("Geolocation")
                .type(PropertyType.GEO_LOCATION)
                .userVisible(true)
                .intents(SchemaConstants.INTENT_GEOLOCATION)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.GEOSHAPE_PROPERTY.getPropertyName())
                .displayName("Geoshape")
                .type(PropertyType.GEO_SHAPE)
                .userVisible(false)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image, video)
                .name(MediaBcSchema.MEDIA_DATE_TAKEN.getPropertyName())
                .displayName("Date taken")
                .type(PropertyType.DATE)
                .userVisible(true)
                .intents(SchemaConstants.INTENT_MEDIA_DATE_TAKEN)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image, video)
                .name(MediaBcSchema.MEDIA_DEVICE_MAKE.getPropertyName())
                .displayName("Device Make")
                .type(PropertyType.STRING)
                .userVisible(true)
                .textIndexHints(TextIndexHint.ALL)
                .intents(SchemaConstants.INTENT_MEDIA_DEVICE_MAKE)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image, video)
                .name(MediaBcSchema.MEDIA_DEVICE_MODEL.getPropertyName())
                .displayName("Device Model")
                .type(PropertyType.STRING)
                .userVisible(true)
                .textIndexHints(TextIndexHint.ALL)
                .intents(SchemaConstants.INTENT_MEDIA_DEVICE_MODEL)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image, video)
                .name(MediaBcSchema.MEDIA_WIDTH.getPropertyName())
                .displayName("Media Width")
                .type(PropertyType.INTEGER)
                .userVisible(true)
                .intents(SchemaConstants.INTENT_MEDIA_WIDTH)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image, video)
                .name(MediaBcSchema.MEDIA_HEIGHT.getPropertyName())
                .displayName("Media Height")
                .type(PropertyType.INTEGER)
                .userVisible(true)
                .intents(SchemaConstants.INTENT_MEDIA_HEIGHT)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image, video)
                .name(MediaBcSchema.MEDIA_METADATA.getPropertyName())
                .displayName("Media Metadata")
                .type(PropertyType.STRING)
                .userVisible(false)
                .textIndexHints(EnumSet.of(TextIndexHint.FULL_TEXT))
                .intents(SchemaConstants.INTENT_MEDIA_METADATA)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image, video)
                .name(MediaBcSchema.MEDIA_FILE_SIZE.getPropertyName())
                .displayName("Media File Size")
                .displayType(SchemaConstants.CUSTOM_DISPLAY_BYTE)
                .type(PropertyType.INTEGER)
                .userVisible(true)
                .intents(SchemaConstants.INTENT_MEDIA_FILE_SIZE)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(audio, image, video)
                .name(MediaBcSchema.MEDIA_DESCRIPTION.getPropertyName())
                .displayName("Media Description")
                .type(PropertyType.STRING)
                .userVisible(true)
                .textIndexHints(TextIndexHint.ALL)
                .intents(SchemaConstants.INTENT_MEDIA_DESCRIPTION)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image)
                .name(MediaBcSchema.MEDIA_IMAGE_HEADING.getPropertyName())
                .displayName("Image Heading")
                .displayType(SchemaConstants.CUSTOM_DISPLAY_HEADING)
                .type(PropertyType.DOUBLE)
                .userVisible(true)
                .intents(SchemaConstants.INTENT_MEDIA_IMAGE_HEADING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image)
                .name(MediaBcSchema.MEDIA_Y_AXIS_FLIPPED.getPropertyName())
                .displayName("Y Axis Flipped")
                .type(PropertyType.BOOLEAN)
                .intents(SchemaConstants.INTENT_MEDIA_Y_AXIS_FLIPPED)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image)
                .name(MediaBcSchema.MEDIA_CLOCKWISE_ROTATION.getPropertyName())
                .displayName("Clockwise Rotation")
                .type(PropertyType.INTEGER)
                .intents(SchemaConstants.INTENT_MEDIA_CLOCKWISE_ROTATION)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(video)
                .name(MediaBcSchema.MEDIA_VIDEO_FORMAT.getPropertyName())
                .displayName("Video Format")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(video)
                .name(MediaBcSchema.MEDIA_VIDEO_CODEC.getPropertyName())
                .displayName("Video Codec")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(video, audio)
                .name(MediaBcSchema.MEDIA_AUDIO_FORMAT.getPropertyName())
                .displayName("Audio Format")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(video, audio)
                .name(MediaBcSchema.MEDIA_AUDIO_CODEC.getPropertyName())
                .displayName("Audio Codec")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(image, video)
                .name(MediaBcSchema.IMAGE_TAG.getPropertyName())
                .displayName("Media Tag")
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.FULL_TEXT))
                .userVisible(true)
                .searchFacet(true)
                .aggType("term")
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.AUTHOR.getPropertyName())
                .textIndexHints(TextIndexHint.ALL)
                .displayName("Author")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.AUTHOR_ID.getPropertyName())
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .displayName("Author ID")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.AUTHOR_REFERENCE.getPropertyName())
                .textIndexHints(TextIndexHint.ALL)
                .displayName("Author Reference")
                .displayType(SchemaConstants.CUSTOM_DISPLAY_LINK)
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.LINKS.getPropertyName())
                .textIndexHints(TextIndexHint.ALL)
                .displayName("Links")
                .displayType(SchemaConstants.CUSTOM_DISPLAY_LINK)
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.HASHTAGS.getPropertyName())
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .displayName("Hashtags")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.PAGE_COUNT.getPropertyName())
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .displayName("Page Count")
                .userVisible(true)
                .type(PropertyType.INTEGER)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(RawObjectSchema.RAW_LANGUAGE.getPropertyName())
                .displayName("Language")
                .type(PropertyType.STRING)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .userVisible(true)
                .possibleValues(new HashMap<String, String>() {{
                    put("en", "English");
                    put("fr", "French");
                    put("de", "German");
                    put("it", "Italian");
                    put("ja", "Japanese");
                    put("pt", "Portuguese");
                    put("es", "Spanish");
                    put("ko", "Korean");
                    put("ro", "Romanian");
                    put("tg", "Tagalog");
                    put("id", "Indonesian");
                    put("zh", "Chinese (Simplified)");
                    put("zh-Hant", "Chinese (Traditional)");
                }})
                .save();
    }

    private void createSpongeOntology() {
        Concept raw = schemaRepository.getConceptByName(SchemaConstants.CONCEPT_TYPE_RAW);
        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.SOURCE_DATE.getPropertyName())
                .displayName("Source Date")
                .type(PropertyType.DATE)
                .sortable(true)
                .userVisible(true)
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .save();

        Concept socialPost = schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_SOCIAL_POST)
                .displayName("Social Post")
                .parent(raw)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_TYPE.getPropertyName(), stringValue(SchemaConstants.DISPLAY_TYPE_IMAGE))
                .glyphIcon("socialpost.png")
                .save();

        schemaFactory.newConceptProperty()
                .concepts(socialPost)
                .name(RawObjectSchema.ORIGINAL_POST_ID.getPropertyName())
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .displayName("Original Post ID")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(socialPost)
                .name(RawObjectSchema.ORIGINAL_POST.getPropertyName())
                .textIndexHints(TextIndexHint.NONE)
                .displayType(SchemaConstants.CUSTOM_DISPLAY_LINK)
                .displayName("Original Post Link")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(raw)
                .name(RawObjectSchema.ORIGINAL_AUTHOR_ID.getPropertyName())
                .textIndexHints(EnumSet.of(TextIndexHint.EXACT_MATCH))
                .displayName("Original Author ID")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(socialPost)
                .name(RawObjectSchema.ORIGINAL_AUTHOR.getPropertyName())
                .textIndexHints(TextIndexHint.NONE)
                .displayType(SchemaConstants.CUSTOM_DISPLAY_LINK)
                .displayName("Original Author Link")
                .userVisible(true)
                .type(PropertyType.STRING)
                .save();

        Concept socialComment = schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_SOCIAL_COMMENT)
                .displayName("Social Comment")
                .parent(raw)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_TYPE.getPropertyName(), stringValue(SchemaConstants.DISPLAY_TYPE_IMAGE))
                .glyphIcon("socialcomment.png")
                .save();

        schemaFactory.newRelationship()
                .label(SchemaConstants.EDGE_LABEL_HAS_SOCIAL_COMMENT)
                .parent(topObjectProperty)
                .source(socialPost)
                .target(socialComment)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Has Social Comment"))
                .save();
    }

    public void createNerOntology() {
        schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_PERSON)
                .displayName("Person")
                .parent(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .glyphIcon("person.png")
                .save();

        schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_ORGANIZATION)
                .displayName("Organization")
                .parent(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .glyphIcon("organization.png")
                .save();

        schemaFactory.newConcept()
                .conceptType(SchemaConstants.CONCEPT_TYPE_LOCATION)
                .displayName("Location")
                .parent(thingConcept)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .glyphIcon("location.png")
                .save();
    }

    public void createFaceRecognitionOntology() {
        Concept person = schemaRepository.getConceptByName(SchemaConstants.CONCEPT_TYPE_PERSON);
        Concept event = schemaRepository.getConceptByName(SchemaConstants.CONCEPT_TYPE_EVENT);
        Concept image = schemaRepository.getConceptByName(SchemaConstants.CONCEPT_TYPE_IMAGE);

        schemaFactory.newConceptProperty()
                .concepts(person)
                .name(FaceRecognitionSchema.FACE_DESCRIPTOR.getPropertyName())
                .displayName("Face Descriptors")
                .type(PropertyType.STRING)
                .userVisible(true)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(person)
                .name(FaceRecognitionSchema.LAST_FACE_EVENT.getPropertyName())
                .displayName("Face Seen On")
                .type(PropertyType.DATE)
                .userVisible(true)
                .save();

        schemaFactory.newConceptProperty()
                .concepts(person)
                .name(FaceRecognitionSchema.NUMBER_OF_FACES.getPropertyName())
                .displayName("Numer of Faces")
                .type(PropertyType.INTEGER)
                .userVisible(true)
                .save();

        schemaFactory.newRelationship()
                .label(SchemaConstants.EDGE_LABEL_FACE_EVENT)
                .parent(topObjectProperty)
                .source(event)
                .target(person)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Face detection"))
                .save();

        Relationship faceOf = schemaFactory.newRelationship()
                .label(SchemaConstants.EDGE_LABEL_FACE_OF)
                .parent(topObjectProperty)
                .source(image)
                .target(person)
                .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                .property(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue("Face of"))
                .save();

        schemaFactory.newConceptProperty()
                .concepts(thingConcept)
                .name(FaceRecognitionSchema.FACE_PROCESSED.getPropertyName())
                .type(PropertyType.BOOLEAN)
                .forRelationships(faceOf)
                .save();
    }
}
