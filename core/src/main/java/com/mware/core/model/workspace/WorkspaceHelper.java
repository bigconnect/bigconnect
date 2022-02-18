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
 *
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
package com.mware.core.model.workspace;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcAccessDeniedException;
import com.mware.core.exception.BcException;
import com.mware.core.exception.BcResourceNotFoundException;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.clientapi.dto.ClientApiWorkspace;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.ArtifactDetectedObject;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.SandboxStatusUtil;
import com.mware.ge.*;
import com.mware.ge.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mware.ge.util.IterableUtils.toList;

@Singleton
public class WorkspaceHelper {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(WorkspaceHelper.class);
    //TODO fix this key when there's a migration capability
    private static final String DETECTED_OBJECT_MULTI_VALUE_KEY_PREFIX = "com.mware.web.routes.vertex.ResolveDetectedObject";
    private final TermMentionRepository termMentionRepository;
    private final WorkQueueRepository workQueueRepository;
    private final WebQueueRepository webQueueRepository;
    private final Graph graph;
    private final SchemaRepository schemaRepository;
    private final WorkspaceRepository workspaceRepository;
    private final PrivilegeRepository privilegeRepository;
    private String entityHasImageIri;
    private final AuthorizationRepository authorizationRepository;
    private String artifactContainsImageOfEntityIri;

    @Inject
    public WorkspaceHelper(
            TermMentionRepository termMentionRepository,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            Graph graph,
            SchemaRepository schemaRepository,
            WorkspaceRepository workspaceRepository,
            PrivilegeRepository privilegeRepository,
            AuthorizationRepository authorizationRepository,
            Configuration configuration
    ) {
        this.termMentionRepository = termMentionRepository;
        this.workQueueRepository = workQueueRepository;
        this.webQueueRepository = webQueueRepository;
        this.graph = graph;
        this.schemaRepository = schemaRepository;
        this.workspaceRepository = workspaceRepository;
        this.privilegeRepository = privilegeRepository;
        this.authorizationRepository = authorizationRepository;
        this.entityHasImageIri = schemaRepository.getRelationshipNameByIntent("entityHasImage", SchemaRepository.PUBLIC);

        if (this.entityHasImageIri == null) {
            LOGGER.warn("'entityHasImage' intent has not been defined. Please update your ontology.");
        }

        this.artifactContainsImageOfEntityIri = schemaRepository.getRelationshipNameByIntent("artifactContainsImageOfEntity", SchemaRepository.PUBLIC);
        if (this.artifactContainsImageOfEntityIri == null) {
            LOGGER.warn("'artifactContainsImageOfEntity' intent has not been defined. Please update your ontology.");
        }
    }

    public String getWorkspaceIdOrNullIfPublish(
            String workspaceId,
            boolean shouldPublish,
            User user
    ) {
        if (shouldPublish) {
            if (privilegeRepository.hasPrivilege(user, Privilege.PUBLISH)) {
                workspaceId = null;
            } else {
                throw new BcAccessDeniedException(
                        "The publish parameter was sent in the request, but the user does not have publish privilege.",
                        user,
                        "publish"
                );
            }
        } else if (workspaceId == null) {
            throw new BcException("workspaceId parameter required");
        }
        return workspaceId;
    }

    public void unresolveTerm(Vertex termMention, Authorizations authorizations) {
        Vertex outVertex = termMentionRepository.findOutVertex(termMention, authorizations);
        if (outVertex == null) {
            return;
        }

        String resolveEdgeId = BcSchema.TERM_MENTION_RESOLVED_EDGE_ID.getPropertyValue(termMention, null);
        if (resolveEdgeId != null) {
            Edge resolveEdge = graph.getEdge(resolveEdgeId, authorizations);
            if (resolveEdge != null) {
                long beforeDeletionTimestamp = System.currentTimeMillis() - 1;
                graph.deleteEdge(resolveEdge, authorizations);
                graph.flush();

                webQueueRepository.broadcastEdgeDeletion(resolveEdge);

                webQueueRepository.broadcastPropertyChange(resolveEdge, null, null, null);

                workQueueRepository.pushOnDwQueue(
                        resolveEdge,
                        null,
                        null,
                        null,
                        null,
                        Priority.HIGH,
                        ElementOrPropertyStatus.DELETION,
                        beforeDeletionTimestamp
                );
            }
        }

        termMentionRepository.delete(termMention, authorizations);
        webQueueRepository.pushTextUpdated(outVertex.getId());

        graph.flush();

    }

    public void deleteProperty(
            Element e,
            Property property,
            boolean propertyIsPublic,
            String workspaceId,
            Priority priority,
            Authorizations authorizations,
            User user
    ) {
        boolean workspaceStaging = workspaceRepository.isStagingEnabled(workspaceId, user);
        if (!workspaceStaging) {
            workspaceId = null;
        }

        if (propertyIsPublic && workspaceId != null) {
            e.markPropertyHidden(property, new Visibility(workspaceId), authorizations);
        } else {
            e.softDeleteProperty(property.getKey(), property.getName(), property.getVisibility(), authorizations);
        }

        if (e instanceof Vertex) {
            unresolveTermMentionsForProperty((Vertex) e, property, authorizations);
        }

        graph.flush();

        if (webQueueRepository.shouldBroadcastGraphPropertyChange(property.getName(), priority)) {
            webQueueRepository.broadcastPropertyChange(e, property.getKey(), property.getName(), workspaceId);
        }
    }

    public void deleteEdge(
            String workspaceId,
            Edge edge,
            Vertex outVertex,
            Vertex inVertex,
            boolean isPublicEdge,
            Priority priority,
            Authorizations authorizations,
            User user
    ) {
        ensureOntologyIrisInitialized();
        long beforeActionTimestamp = System.currentTimeMillis() - 1;

        deleteProperties(edge, workspaceId, priority, authorizations, user);
        unresolveDetectedObjects(workspaceId, edge, outVertex, inVertex, priority, authorizations);

        boolean workspaceStaging = workspaceRepository.isStagingEnabled(workspaceId, user);
        if (!workspaceStaging) {
            // add the vertex to the workspace so that the changes show up in the diff panel
            workspaceRepository.updateEntityOnWorkspace(workspaceId, edge.getVertexId(Direction.IN), user);
            workspaceRepository.updateEntityOnWorkspace(workspaceId, edge.getVertexId(Direction.OUT), user);
        }

        if (isPublicEdge) {
            Visibility workspaceVisibility = new Visibility(workspaceId);

            graph.markEdgeHidden(edge, workspaceVisibility, authorizations);

            if (edge.getLabel().equals(entityHasImageIri)) {
                Property entityHasImage = outVertex.getProperty(RawObjectSchema.ENTITY_IMAGE_VERTEX_ID.getPropertyName());
                if (entityHasImage != null) {
                    outVertex.markPropertyHidden(entityHasImage, workspaceVisibility, authorizations);
                    workQueueRepository.pushElementImageQueue(outVertex, entityHasImage.getKey(), entityHasImage.getName(), priority);
                    webQueueRepository.broadcastElementImage(outVertex);
                }
            }

            for (Vertex termMention : termMentionRepository.findByEdgeId(
                    outVertex.getId(),
                    edge.getId(),
                    authorizations
            )) {
                termMentionRepository.markHidden(termMention, workspaceVisibility, authorizations);
                webQueueRepository.pushTextUpdated(outVertex.getId());
            }

            if (!workspaceStaging) {
                long beforeDeletionTimestamp = System.currentTimeMillis() - 1;
                graph.deleteEdge(edge, authorizations);
                graph.flush();

                if (webQueueRepository.shouldBroadcastGraphPropertyChange(null, priority)) {
                    webQueueRepository.broadcastPropertyChange(edge, null, null, workspaceId);
                }
                this.workQueueRepository.pushOnDwQueue(edge, null, null, null, null, priority, ElementOrPropertyStatus.DELETION, beforeDeletionTimestamp);
            } else {
                graph.flush();
                webQueueRepository.broadcastEdgeDeletion(edge);
                webQueueRepository.broadcastPropertyChange(edge, null, null, workspaceId);
                workQueueRepository.pushOnDwQueue(edge, null, null, null, null, Priority.HIGH, ElementOrPropertyStatus.HIDDEN, beforeActionTimestamp);
            }
        } else {
            graph.softDeleteEdge(edge, authorizations);

            if (edge.getLabel().equals(entityHasImageIri)) {
                Property entityHasImage = outVertex.getProperty(RawObjectSchema.ENTITY_IMAGE_VERTEX_ID.getPropertyName());
                if (entityHasImage != null) {
                    outVertex.softDeleteProperty(entityHasImage.getKey(), entityHasImage.getName(), authorizations);
                    workQueueRepository.pushElementImageQueue(outVertex, entityHasImage.getKey(), entityHasImage.getName(), priority);
                    webQueueRepository.broadcastElementImage(outVertex);
                }
            }

            for (Vertex termMention : termMentionRepository.findByEdgeId(
                    outVertex.getId(),
                    edge.getId(),
                    authorizations
            )) {
                termMentionRepository.delete(termMention, authorizations);
                webQueueRepository.pushTextUpdated(outVertex.getId());
            }

            if (!workspaceStaging) {
                long beforeDeletionTimestamp = System.currentTimeMillis() - 1;
                graph.softDeleteEdge(edge, authorizations);
                graph.flush();

                if(webQueueRepository.shouldBroadcast(priority)) {
                    webQueueRepository.broadcastPropertyChange(edge, null, null, null);
                }
                workQueueRepository.pushOnDwQueue(edge, null, null, null, null, priority, ElementOrPropertyStatus.DELETION, beforeDeletionTimestamp);
            } else {
                graph.flush();

                webQueueRepository.broadcastEdgeDeletion(edge);
                webQueueRepository.broadcastPropertyChange(edge, null, null, null);
                workQueueRepository.pushOnDwQueue(
                        edge,
                        null, null, null, null,
                        Priority.HIGH,
                        ElementOrPropertyStatus.DELETION,
                        beforeActionTimestamp
                );
            }
        }
    }

    private void ensureOntologyIrisInitialized() {
        if (this.entityHasImageIri == null) {
            this.entityHasImageIri = schemaRepository.getRelationshipNameByIntent("entityHasImage", SchemaRepository.PUBLIC);
        }
        if (this.artifactContainsImageOfEntityIri == null) {
            this.artifactContainsImageOfEntityIri = schemaRepository.getRelationshipNameByIntent("artifactContainsImageOfEntity", SchemaRepository.PUBLIC);
        }
    }

    public void deleteProperties(
            Element e,
            String propertyKey,
            String propertyName,
            SchemaProperty schemaProperty,
            String workspaceId,
            Authorizations authorizations, User user
    ) {
        List<Property> properties = new ArrayList<>();
        properties.addAll(toList(e.getProperties(propertyKey, propertyName)));

        if (schemaProperty != null) {
            for (String dependentPropertyNames : schemaProperty.getDependentPropertyNames()) {
                properties.addAll(toList(e.getProperties(propertyKey, dependentPropertyNames)));
            }
        }

        if (properties.size() == 0) {
            throw new BcResourceNotFoundException(String.format(
                    "Could not find property %s:%s on %s",
                    propertyName,
                    propertyKey,
                    e
            ));
        }

        if (workspaceId != null) {
            if (e instanceof Edge) {
                Edge edge = (Edge) e;
                // add the vertex to the workspace so that the changes show up in the diff panel
                workspaceRepository.updateEntityOnWorkspace(
                        workspaceId,
                        edge.getVertexId(Direction.IN),
                        user
                );
                workspaceRepository.updateEntityOnWorkspace(
                        workspaceId,
                        edge.getVertexId(Direction.OUT),
                        user
                );
            } else if (e instanceof Vertex) {
                // add the vertex to the workspace so that the changes show up in the diff panel
                workspaceRepository.updateEntityOnWorkspace(workspaceId, e.getId(), user);
            } else {
                throw new BcException("element is not an edge or vertex: " + e);
            }
        }

        SandboxStatus[] sandboxStatuses = SandboxStatusUtil.getPropertySandboxStatuses(properties, workspaceId);

        for (int i = 0; i < sandboxStatuses.length; i++) {
            boolean propertyIsPublic = (sandboxStatuses[i] == SandboxStatus.PUBLIC);
            Property property = properties.get(i);
            deleteProperty(e, property, propertyIsPublic, workspaceId, Priority.HIGH, authorizations, user);
        }
    }

    private void deleteProperties(Element e, String workspaceId, Priority priority, Authorizations authorizations, User user) {
        List<Property> properties = IterableUtils.toList(e.getProperties());
        SandboxStatus[] sandboxStatuses = SandboxStatusUtil.getPropertySandboxStatuses(properties, workspaceId);

        for (int i = 0; i < sandboxStatuses.length; i++) {
            boolean propertyIsPublic = (sandboxStatuses[i] == SandboxStatus.PUBLIC);
            Property property = properties.get(i);
            deleteProperty(e, property, propertyIsPublic, workspaceId, priority, authorizations, user);
        }
    }

    public void deleteVertex(
            Vertex vertex,
            String workspaceId,
            boolean isPublicVertex,
            Priority priority,
            Authorizations authorizations,
            User user
    ) {
        LOGGER.debug(
                "BEGIN deleteVertex(vertexId: %s, workspaceId: %s, isPublicVertex: %b, user: %s)",
                vertex.getId(),
                workspaceId,
                isPublicVertex,
                user.getUsername()
        );
        ensureOntologyIrisInitialized();
        long beforeActionTimestamp = System.currentTimeMillis() - 1;

        deleteProperties(vertex, workspaceId, priority, authorizations, user);

        // make sure the entity is on the workspace so that it shows up in the diff panel
        boolean workspaceStaging = workspaceRepository.isStagingEnabled(workspaceId, user);
        if (!workspaceStaging) {
            workspaceRepository.updateEntityOnWorkspace(workspaceId, vertex.getId(), user);
        }
        VisibilityJson visibilityJson = BcSchema.VISIBILITY_JSON.getPropertyValue(vertex);
        VisibilityJson.removeFromAllWorkspace(visibilityJson);

        // because we store the current vertex image in a property we need to possibly find that property and change it
        // if we are deleting the current image.
        LOGGER.debug("change entity image properties");
        for (Edge edge : vertex.getEdges(Direction.BOTH, entityHasImageIri, authorizations)) {
            if (edge.getVertexId(Direction.IN).equals(vertex.getId())) {
                Vertex outVertex = edge.getVertex(Direction.OUT, authorizations);
                Property entityHasImage = outVertex.getProperty(RawObjectSchema.ENTITY_IMAGE_VERTEX_ID.getPropertyName());
                outVertex.softDeleteProperty(entityHasImage.getKey(), entityHasImage.getName(), authorizations);
                workQueueRepository.pushElementImageQueue(outVertex, entityHasImage.getKey(), entityHasImage.getName(), priority);
                webQueueRepository.broadcastElementImage(outVertex);
                graph.softDeleteEdge(edge, authorizations);

                webQueueRepository.broadcastEdgeDeletion(edge);
                webQueueRepository.broadcastPropertyChange(edge, null, null, null);
            }
        }

        // because detected objects are currently stored as properties on the artifact that reference the entity
        //   that they are resolved to we need to delete that property
        LOGGER.debug("change artifact contains image of entity");
        for (Edge edge : vertex.getEdges(Direction.BOTH, artifactContainsImageOfEntityIri, authorizations)) {
            for (Property rowKeyProperty : vertex.getProperties(RawObjectSchema.ROW_KEY.getPropertyName())) {
                String multiValueKey = rowKeyProperty.getValue().toString();
                if (edge.getVertexId(Direction.IN).equals(vertex.getId())) {
                    Vertex outVertex = edge.getVertex(Direction.OUT, authorizations);
                    // remove property
                    RawObjectSchema.DETECTED_OBJECT.removeProperty(outVertex, multiValueKey, authorizations);
                    graph.softDeleteEdge(edge, authorizations);

                    webQueueRepository.broadcastEdgeDeletion(edge);
                    webQueueRepository.broadcastPropertyChange(edge, null, null, null);
                    workQueueRepository.pushOnDwQueue(
                            edge,
                            null, null, null, null,
                            Priority.HIGH,
                            ElementOrPropertyStatus.DELETION,
                            beforeActionTimestamp
                    );

                    if(webQueueRepository.shouldBroadcast(priority)) {
                        webQueueRepository.broadcastPropertyChange(outVertex, multiValueKey, RawObjectSchema.DETECTED_OBJECT.getPropertyName(), workspaceId);
                    }
                }
            }
        }

        // because we store term mentions with an added visibility we need to delete them with that added authorizations.
        //  we also need to notify the front-end of changes as well as audit the changes
        LOGGER.debug("unresolve terms");
        for (Vertex termMention : termMentionRepository.findResolvedTo(vertex.getId(), authorizations)) {
            unresolveTerm(termMention, authorizations);
        }

        Authorizations systemAuthorization = authorizationRepository.getGraphAuthorizations(
                user,
                WorkspaceRepository.VISIBILITY_STRING,
                workspaceId
        );
        if (isPublicVertex) {
            Visibility workspaceVisibility = new Visibility(workspaceId);
            graph.markVertexHidden(vertex, workspaceVisibility, systemAuthorization);
            graph.flush();

            if (!workspaceStaging) {
                long beforeDeletionTimestamp = System.currentTimeMillis() - 1;
                graph.deleteVertex(vertex, authorizations);
                graph.flush();
                webQueueRepository.broadcastPropertyChange(vertex, null, null, null);
                workQueueRepository.pushOnDwQueue(vertex, null, null, null, null, Priority.HIGH, ElementOrPropertyStatus.DELETION, beforeDeletionTimestamp);
            } else {
                webQueueRepository.broadcastVerticesDeletion(vertex.getId());
                webQueueRepository.broadcastPropertyChange(vertex, null, null, null);
                workQueueRepository.pushOnDwQueue(vertex, null, null, null, null, Priority.HIGH, ElementOrPropertyStatus.HIDDEN, beforeActionTimestamp);
            }
        } else {
            // because we store workspaces with an added visibility we need to delete them with that added authorizations.
            LOGGER.debug("soft delete edges");
            Vertex workspaceVertex = graph.getVertex(workspaceId, systemAuthorization);
            for (Edge edge : workspaceVertex.getEdges(vertex, Direction.BOTH, systemAuthorization)) {
                graph.softDeleteEdge(edge, systemAuthorization);
            }

            vertex = graph.getVertex(vertex.getId(), FetchHints.ALL_INCLUDING_HIDDEN, systemAuthorization);
            for (Edge edge : vertex.getEdges(Direction.IN, WebWorkspaceSchema.PRODUCT_TO_ENTITY_RELATIONSHIP_NAME, systemAuthorization)) {
                graph.softDeleteEdge(edge, systemAuthorization);
            }

            LOGGER.debug("soft delete vertex");
            graph.softDeleteVertex(vertex, authorizations);
            graph.flush();

            if (!workspaceStaging) {
                long beforeDeletionTimestamp = System.currentTimeMillis() - 1;
                webQueueRepository.broadcastPropertyChange(vertex, null, null, null);
                workQueueRepository.pushOnDwQueue(vertex, null, null, null, null, Priority.HIGH, ElementOrPropertyStatus.DELETION, beforeDeletionTimestamp);
            } else {
                webQueueRepository.broadcastVerticesDeletion(vertex.getId());
                webQueueRepository.broadcastPropertyChange(vertex, null, null, null);
                workQueueRepository.pushOnDwQueue(vertex, null, null, null, null, Priority.HIGH, ElementOrPropertyStatus.DELETION, beforeActionTimestamp);
            }
        }

        graph.flush();
        LOGGER.debug("END deleteVertex");
    }

    private void unresolveTermMentionsForProperty(Vertex vertex, Property property, Authorizations authorizations) {
        for (Vertex termMention : termMentionRepository.findResolvedTo(vertex.getId(), authorizations)) {
            String key = BcSchema.TERM_MENTION_REF_PROPERTY_KEY.getPropertyValue(termMention);
            String name = BcSchema.TERM_MENTION_REF_PROPERTY_NAME.getPropertyValue(termMention);
            String visibility = BcSchema.TERM_MENTION_REF_PROPERTY_VISIBILITY.getPropertyValue(termMention);
            if (property.getKey().equals(key) && property.getName().equals(name) &&
                    property.getVisibility().getVisibilityString().equals(visibility)) {
                unresolveTerm(termMention, authorizations);
            }
        }
    }

    private void unresolveDetectedObjects(
            String workspaceId,
            Edge edge,
            Vertex outVertex,
            Vertex inVertex,
            Priority priority,
            Authorizations authorizations
    ) {
        for (ArtifactDetectedObject artifactDetectedObject : RawObjectSchema.DETECTED_OBJECT.getPropertyValues(
                outVertex)) {
            if (edge.getId().equals(artifactDetectedObject.getEdgeId())) {
                unresolveDetectedObject(
                        artifactDetectedObject,
                        workspaceId,
                        edge,
                        outVertex,
                        inVertex,
                        priority,
                        authorizations
                );
            }
        }
    }

    private void unresolveDetectedObject(
            ArtifactDetectedObject artifactDetectedObject,
            String workspaceId,
            Edge edge,
            Vertex outVertex,
            Vertex inVertex,
            Priority priority,
            Authorizations authorizations
    ) {
        String multiValueKey = artifactDetectedObject.getMultivalueKey(DETECTED_OBJECT_MULTI_VALUE_KEY_PREFIX);
        SandboxStatus vertexSandboxStatus = SandboxStatusUtil.getSandboxStatus(inVertex, workspaceId);
        VisibilityJson visibilityJson;
        if (vertexSandboxStatus == SandboxStatus.PUBLIC) {
            visibilityJson = BcSchema.VISIBILITY_JSON.getPropertyValue(edge);
            visibilityJson = VisibilityJson.removeFromWorkspace(visibilityJson, workspaceId);
        } else {
            visibilityJson = BcSchema.VISIBILITY_JSON.getPropertyValue(inVertex);
            visibilityJson = VisibilityJson.removeFromWorkspace(visibilityJson, workspaceId);
        }
        RawObjectSchema.DETECTED_OBJECT.removeProperty(outVertex, multiValueKey, authorizations);

        if(webQueueRepository.shouldBroadcast(priority)) {
            webQueueRepository.broadcastPropertyChange(outVertex, multiValueKey, RawObjectSchema.DETECTED_OBJECT.getPropertyName(), workspaceId);
        }
        workQueueRepository.pushOnDwQueue(
                outVertex,
                multiValueKey,
                RawObjectSchema.DETECTED_OBJECT.getPropertyName(),
                workspaceId,
                visibilityJson.getSource(),
                priority,
                ElementOrPropertyStatus.UPDATE,
                null
        );
    }

    public void updateEntitiesOnWorkspace(
            String workspaceId,
            Collection<String> vertexIds,
            User user
    ) {
        LOGGER.debug("Updating vertices on workspace %s: %s",  workspaceId, Joiner.on(",").join(vertexIds));
        Workspace workspace = workspaceRepository.findById(workspaceId, user);
        workspaceRepository.updateEntitiesOnWorkspace(workspace, vertexIds, user);
        webQueueRepository.broadcastUserWorkspaceChange(user, workspaceId);
        graph.flush();
    }

    public void broadcastWorkProductChange(String workspaceId, String productId, String sourceGuid, User user, Authorizations authorizations) {
        Workspace workspace = workspaceRepository.findById(workspaceId, user);
        ClientApiWorkspace clientApiWorkspace = workspaceRepository.toClientApi(workspace, user, authorizations);

        webQueueRepository.broadcastWorkProductChange(
                productId,
                null,
                clientApiWorkspace.getWorkspaceId(),
                webQueueRepository.getPermissionsWithUsers(clientApiWorkspace, null)
        );
    }
}
