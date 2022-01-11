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

import com.google.inject.Inject;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.exception.BcResourceNotFoundException;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.clientapi.dto.ClientApiSourceInfo;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.termMention.TermMentionFor;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.GeMetadataUtil;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.Value;

import java.time.ZonedDateTime;

import static com.google.common.base.Preconditions.checkNotNull;

public class GraphRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(GraphRepository.class);
    public static final String BC_VERSION_KEY = "bc.version";
    public static final int BC_VERSION = 4;
    private final Graph graph;
    private final VisibilityTranslator visibilityTranslator;
    private final TermMentionRepository termMentionRepository;
    private final WorkQueueRepository workQueueRepository;
    private final WebQueueRepository webQueueRepository;

    @Inject
    public GraphRepository(
            Graph graph,
            VisibilityTranslator visibilityTranslator,
            TermMentionRepository termMentionRepository,
            WorkQueueRepository workQueueRepository,
            WebQueueRepository webQueueRepository,
            Configuration configuration
    ) {
        this.graph = graph;
        this.visibilityTranslator = visibilityTranslator;
        this.termMentionRepository = termMentionRepository;
        this.workQueueRepository = workQueueRepository;
        this.webQueueRepository = webQueueRepository;
    }

    public void verifyVersion() {
        verifyVersion(BC_VERSION);
    }

    public void verifyVersion(int requiredVersion) {
        Object version = graph.getMetadata(BC_VERSION_KEY);
        if (version == null) {
            writeVersion();
            return;
        }
        if (!(version instanceof Integer)) {
            throw new BcException("Invalid " + BC_VERSION_KEY + " found. Expected Integer, found " + version.getClass().getName());
        }
        Integer versionInt = (Integer) version;
        if (versionInt != requiredVersion) {
            throw new BcException("Incompatible graph version (" + BC_VERSION_KEY + ") found. Expected " + requiredVersion + ", found " + versionInt);
        }
        LOGGER.info("BigConnect graph version verified: %d", versionInt);
    }

    public void writeVersion() {
        writeVersion(BC_VERSION);
    }

    public void writeVersion(int version) {
        graph.setMetadata(BC_VERSION_KEY, version);
        LOGGER.info("Wrote %s: %d", BC_VERSION_KEY, version);
    }

    public <T extends Element> VisibilityAndElementMutation<T> updateElementVisibilitySource(
            Element element,
            SandboxStatus sandboxStatus,
            String visibilitySource,
            String workspaceId,
            Authorizations authorizations
    ) {
        Visibility defaultVisibility = visibilityTranslator.getDefaultVisibility();
        VisibilityJson visibilityJson = BcSchema.VISIBILITY_JSON.getPropertyValue(element);
        visibilityJson = sandboxStatus != SandboxStatus.PUBLIC
                ? VisibilityJson.updateVisibilitySourceAndAddWorkspaceId(visibilityJson, visibilitySource, workspaceId)
                : VisibilityJson.updateVisibilitySource(visibilityJson, visibilitySource);

        BcVisibility bcVisibility = visibilityTranslator.toVisibility(visibilityJson);
        Visibility visibility = bcVisibility.getVisibility();

        ExistingElementMutation<T> m = element.<T>prepareMutation().alterElementVisibility(visibility);

        if (BcSchema.VISIBILITY_JSON.hasProperty(element)) {
            Property visibilityJsonProperty = BcSchema.VISIBILITY_JSON.getProperty(element);
            m.alterPropertyVisibility(
                    visibilityJsonProperty.getKey(), BcSchema.VISIBILITY_JSON.getPropertyName(),
                    defaultVisibility
            );
        }
        BcSchema.VISIBILITY_JSON.setProperty(m, visibilityJson, defaultVisibility);

        m.save(authorizations);
        return new VisibilityAndElementMutation<>(bcVisibility, m);
    }

    public <T extends Element> Property updatePropertyVisibilitySource(
            Element element,
            String propertyKey,
            String propertyName,
            String oldVisibilitySource,
            String newVisibilitySource,
            String workspaceId,
            User user,
            Authorizations authorizations
    ) {
        Visibility defaultVisibility = visibilityTranslator.getDefaultVisibility();
        Property property = getProperty(element, propertyKey, propertyName, oldVisibilitySource, workspaceId);
        if (property == null) {
            throw new BcResourceNotFoundException("Could not find property " + propertyKey + ":" + propertyName + " on element " + element.getId());
        }

        VisibilityJson newVisibilityJson = new VisibilityJson(newVisibilitySource);
        Visibility newVisibility = visibilityTranslator.toVisibility(newVisibilityJson).getVisibility();

        LOGGER.info(
                "%s Altering property visibility %s [%s:%s] from [%s] to [%s]",
                user.getUserId(),
                element.getId(),
                propertyKey,
                propertyName,
                oldVisibilitySource,
                newVisibility.toString()
        );

        ExistingElementMutation<T> m = element.<T>prepareMutation()
                .alterPropertyVisibility(property, newVisibility);
        BcSchema.VISIBILITY_JSON_METADATA.setMetadata(m, property, newVisibilityJson, defaultVisibility);
        T newElement = m.save(authorizations);

        Property newProperty = newElement.getProperty(propertyKey, propertyName, newVisibility);
        checkNotNull(
                newProperty,
                "Could not find altered property " + propertyKey + ":" + propertyName + " on element " + element.getId()
        );

        return newProperty;
    }

    private Property getProperty(
            Element element,
            String propertyKey,
            String propertyName,
            String visibilitySource,
            String workspaceId
    ) {
        Property property = element.getProperty(
                propertyKey,
                propertyName,
                getVisibilityWithWorkspace(visibilitySource, workspaceId)
        );

        // could be a public property, let's try fetching it without workspace id
        if (property == null) {
            property = element.getProperty(
                    propertyKey,
                    propertyName,
                    getVisibilityWithWorkspace(visibilitySource, null)
            );
        }

        return property;
    }

    public <T extends Element> VisibilityAndElementMutation<T> setProperty(
            T element,
            String propertyName,
            String propertyKey,
            Value value,
            Metadata metadata,
            String oldVisibilitySource,
            String newVisibilitySource,
            String workspaceId,
            String justificationText,
            ClientApiSourceInfo sourceInfo,
            User user,
            Authorizations authorizations
    ) {
        WorkspaceRepository workspaceRepository = InjectHelper.getInstance(WorkspaceRepository.class);
        boolean workspaceStaging = workspaceRepository.isStagingEnabled(workspaceId, user);
        if (!workspaceStaging) {
            workspaceId = null;
        }
        Visibility defaultVisibility = visibilityTranslator.getDefaultVisibility();

        Visibility oldPropertyVisibility = getVisibilityWithWorkspace(oldVisibilitySource, workspaceId);
        Property oldProperty = element.getProperty(propertyKey, propertyName, oldPropertyVisibility);
        boolean isUpdate = oldProperty != null;

        Metadata propertyMetadata = isUpdate ? oldProperty.getMetadata() : Metadata.create();
        propertyMetadata = GeMetadataUtil.mergeMetadata(propertyMetadata, metadata);

        ExistingElementMutation<T> elementMutation = element.prepareMutation();

        VisibilityJson visibilityJson = VisibilityJson.updateVisibilitySourceAndAddWorkspaceId(
                null,
                newVisibilitySource,
                workspaceId
        );
        BcSchema.VISIBILITY_JSON_METADATA.setMetadata(propertyMetadata, visibilityJson, defaultVisibility);
        BcSchema.MODIFIED_DATE_METADATA.setMetadata(propertyMetadata, ZonedDateTime.now(), defaultVisibility);
        BcSchema.MODIFIED_BY_METADATA.setMetadata(propertyMetadata, user.getUserId(), defaultVisibility);

        BcVisibility bcVisibility = visibilityTranslator.toVisibility(visibilityJson);
        Visibility propertyVisibility = bcVisibility.getVisibility();

        if (justificationText != null) {
            termMentionRepository.removeSourceInfoEdge(
                    element,
                    propertyKey,
                    propertyName,
                    bcVisibility,
                    authorizations
            );
            BcSchema.JUSTIFICATION_METADATA.setMetadata(
                    propertyMetadata,
                    justificationText,
                    defaultVisibility
            );
        } else if (sourceInfo != null) {
            Vertex outVertex = graph.getVertex(sourceInfo.vertexId, authorizations);
            BcSchema.JUSTIFICATION.removeMetadata(propertyMetadata);
            termMentionRepository.addSourceInfo(
                    element,
                    element.getId(),
                    TermMentionFor.PROPERTY,
                    propertyKey,
                    propertyName,
                    propertyVisibility,
                    sourceInfo.snippet,
                    sourceInfo.textPropertyKey,
                    sourceInfo.textPropertyName,
                    sourceInfo.startOffset,
                    sourceInfo.endOffset,
                    outVertex,
                    propertyVisibility,
                    authorizations
            );
        }

        Property publicProperty = element.getProperty(propertyKey, propertyName);
        // only public properties in a workspace will be sandboxed (hidden from the workspace)
        if (publicProperty != null && workspaceId != null &&
                SandboxStatus.getFromVisibilityString(publicProperty.getVisibility().getVisibilityString(), workspaceId)
                        == SandboxStatus.PUBLIC) {
            long beforeDeletionTimestamp = System.currentTimeMillis() - 1;
            // changing a public property, so hide it from the workspace
            element.markPropertyHidden(publicProperty, new Visibility(workspaceId), authorizations);
            graph.flush();

            if (webQueueRepository.shouldBroadcastGraphPropertyChange(publicProperty.getName(), Priority.HIGH)) {
                webQueueRepository.broadcastPropertyChange(element, publicProperty.getKey(), publicProperty.getName(), workspaceId);
            }

            workQueueRepository.pushOnDwQueue(
                    element,
                    publicProperty.getKey(),
                    publicProperty.getName(),
                    workspaceId,
                    null,
                    Priority.HIGH,
                    ElementOrPropertyStatus.HIDDEN,
                    beforeDeletionTimestamp);

        } else if (isUpdate && oldVisibilitySource != null && !oldVisibilitySource.equals(newVisibilitySource)) {
            // changing a existing sandboxed property's visibility
            elementMutation.alterPropertyVisibility(oldProperty, propertyVisibility);
        }

        elementMutation.addPropertyValue(propertyKey, propertyName, value, propertyMetadata, propertyVisibility);

        return new VisibilityAndElementMutation<>(bcVisibility, elementMutation);
    }

    private Visibility getVisibilityWithWorkspace(String visibilitySource, String workspaceId) {
        Visibility visibility = null;
        if (visibilitySource != null) {
            VisibilityJson oldVisibilityJson = new VisibilityJson();
            oldVisibilityJson.setSource(visibilitySource);
            oldVisibilityJson.addWorkspace(workspaceId);
            visibility = visibilityTranslator.toVisibility(oldVisibilityJson).getVisibility();
        }
        return visibility;
    }

    public Vertex addVertex(
            String vertexId,
            String conceptType,
            String visibilitySource,
            String workspaceId,
            String justificationText,
            ClientApiSourceInfo sourceInfo,
            User user,
            Authorizations authorizations
    ) {
        WorkspaceRepository workspaceRepository = InjectHelper.getInstance(WorkspaceRepository.class);
        boolean workspaceStaging = workspaceRepository.isStagingEnabled(workspaceId, user);

        if (!workspaceStaging) {
            workspaceId = null;
        }
        VisibilityJson visibilityJson = VisibilityJson.updateVisibilitySourceAndAddWorkspaceId(
                null,
                visibilitySource,
                workspaceId
        );
        return addVertex(vertexId, conceptType, visibilityJson, justificationText, sourceInfo, user, authorizations);
    }

    public Vertex addVertex(
            String vertexId,
            String conceptType,
            VisibilityJson visibilityJson,
            String justificationText,
            ClientApiSourceInfo sourceInfo,
            User user,
            Authorizations authorizations
    ) {
        BcVisibility bcVisibility = visibilityTranslator.toVisibility(visibilityJson);
        VertexBuilder vertexBuilder;
        if (vertexId != null) {
            vertexBuilder = graph.prepareVertex(vertexId, bcVisibility.getVisibility(), conceptType);
        } else {
            vertexBuilder = graph.prepareVertex(bcVisibility.getVisibility(), conceptType);
        }
        updateElementMetadataProperties(vertexBuilder, visibilityJson, user);

        boolean justificationAdded = addJustification(
                vertexBuilder,
                justificationText,
                bcVisibility,
                visibilityJson,
                user
        );

        Vertex vertex = vertexBuilder.save(authorizations);
        graph.flush();

        if (justificationAdded) {
            termMentionRepository.removeSourceInfoEdgeFromVertex(
                    vertex.getId(),
                    vertex.getId(),
                    null,
                    null,
                    bcVisibility,
                    authorizations
            );
        } else if (sourceInfo != null) {
            BcSchema.JUSTIFICATION.removeProperty(vertexBuilder, bcVisibility.getVisibility());

            Vertex sourceDataVertex = graph.getVertex(sourceInfo.vertexId, authorizations);
            termMentionRepository.addSourceInfoToVertex(
                    vertex,
                    vertex.getId(),
                    TermMentionFor.VERTEX,
                    null,
                    null,
                    null,
                    sourceInfo.snippet,
                    sourceInfo.textPropertyKey,
                    sourceInfo.textPropertyName,
                    sourceInfo.startOffset,
                    sourceInfo.endOffset,
                    sourceDataVertex,
                    bcVisibility.getVisibility(),
                    authorizations
            );
        }

        return vertex;
    }

    public Edge addEdge(
            String edgeId,
            Vertex outVertex,
            Vertex inVertex,
            String predicateLabel,
            String justificationText,
            ClientApiSourceInfo sourceInfo,
            String visibilitySource,
            String workspaceId,
            User user,
            Authorizations authorizations
    ) {
        WorkspaceRepository workspaceRepository = InjectHelper.getInstance(WorkspaceRepository.class);
        boolean workspaceStaging = workspaceRepository.isStagingEnabled(workspaceId, user);

        if (!workspaceStaging) {
            workspaceId = null;
        }
        VisibilityJson visibilityJson = VisibilityJson.updateVisibilitySourceAndAddWorkspaceId(
                null,
                visibilitySource,
                workspaceId
        );
        return addEdge(edgeId, outVertex, inVertex, predicateLabel, justificationText, sourceInfo, visibilityJson, user, authorizations);
    }

    public Edge addEdge(
            String edgeId,
            Vertex outVertex,
            Vertex inVertex,
            String predicateLabel,
            String justificationText,
            ClientApiSourceInfo sourceInfo,
            VisibilityJson visibilityJson,
            User user,
            Authorizations authorizations
    ) {
        BcVisibility bcVisibility = visibilityTranslator.toVisibility(visibilityJson);
        ElementBuilder<Edge> edgeBuilder;
        if (edgeId == null) {
            edgeBuilder = graph.prepareEdge(outVertex, inVertex, predicateLabel, bcVisibility.getVisibility());
        } else {
            edgeBuilder = graph.prepareEdge(
                    edgeId,
                    outVertex,
                    inVertex,
                    predicateLabel,
                    bcVisibility.getVisibility()
            );
        }
        updateElementMetadataProperties(edgeBuilder, visibilityJson, user);

        boolean justificationAdded = addJustification(
                edgeBuilder,
                justificationText,
                bcVisibility,
                visibilityJson,
                user
        );

        Edge edge = edgeBuilder.save(authorizations);

        if (justificationAdded) {
            termMentionRepository.removeSourceInfoEdgeFromEdge(edge, null, null, bcVisibility, authorizations);
        } else if (sourceInfo != null) {
            BcSchema.JUSTIFICATION.removeProperty(edgeBuilder, bcVisibility.getVisibility());

            Vertex sourceDataVertex = graph.getVertex(sourceInfo.vertexId, authorizations);
            termMentionRepository.addSourceInfoEdgeToEdge(
                    edge,
                    edge.getId(),
                    TermMentionFor.EDGE,
                    null,
                    null,
                    null,
                    sourceInfo.snippet,
                    sourceInfo.textPropertyKey,
                    sourceInfo.textPropertyName,
                    sourceInfo.startOffset,
                    sourceInfo.endOffset,
                    sourceDataVertex,
                    bcVisibility.getVisibility(),
                    authorizations
            );
        }

        return edge;
    }

    private void updateElementMetadataProperties(
            ElementBuilder elementBuilder,
            VisibilityJson visibilityJson,
            User user
    ) {
        Visibility defaultVisibility = visibilityTranslator.getDefaultVisibility();

        BcSchema.VISIBILITY_JSON.setProperty(
                elementBuilder,
                visibilityJson,
                defaultVisibility
        );
        BcSchema.MODIFIED_DATE.setProperty(elementBuilder, ZonedDateTime.now(), defaultVisibility);
        BcSchema.MODIFIED_BY.setProperty(elementBuilder, user.getUserId(), defaultVisibility);
    }

    private boolean addJustification(
            ElementBuilder elementBuilder,
            String justificationText,
            BcVisibility bcVisibility,
            VisibilityJson visibilityJson,
            User user
    ) {
        Visibility visibility = bcVisibility.getVisibility();
        if (justificationText != null) {
            Metadata metadata = Metadata.create();
            Visibility metadataVisibility = visibilityTranslator.getDefaultVisibility();
            BcSchema.MODIFIED_DATE_METADATA.setMetadata(metadata, ZonedDateTime.now(), metadataVisibility);
            BcSchema.MODIFIED_BY_METADATA.setMetadata(metadata, user.getUserId(), metadataVisibility);
            BcSchema.VISIBILITY_JSON_METADATA.setMetadata(metadata, visibilityJson, metadataVisibility);

            BcSchema.JUSTIFICATION.setProperty(elementBuilder, justificationText, metadata, visibility);
            return true;
        }
        return false;
    }

    public GraphUpdateContext beginGraphUpdate(Priority priority, User user, Authorizations authorizations) {
        return new MyGraphUpdateContext(
                graph,
                workQueueRepository,
                webQueueRepository,
                visibilityTranslator,
                priority,
                user,
                authorizations
        );
    }

    private static class MyGraphUpdateContext extends GraphUpdateContext {
        protected MyGraphUpdateContext(
                Graph graph,
                WorkQueueRepository workQueueRepository,
                WebQueueRepository webQueueRepository,
                VisibilityTranslator visibilityTranslator,
                Priority priority,
                User user,
                Authorizations authorizations
        ) {
            super(graph, workQueueRepository, webQueueRepository, visibilityTranslator, priority, user, authorizations);
        }
    }
}
