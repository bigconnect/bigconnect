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

import com.google.inject.Inject;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.video.VideoTranscript;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.MediaBcSchema;
import com.mware.core.model.properties.types.BcPropertyUpdate;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.RowKeyHelper;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.values.storable.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

public abstract class DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(DataWorker.class);
    private Graph graph;
    private VisibilityTranslator visibilityTranslator;
    private WorkQueueRepository workQueueRepository;
    private WebQueueRepository webQueueRepository;
    private SchemaRepository schemaRepository;
    private GraphAuthorizationRepository graphAuthorizationRepository;
    private DataWorkerPrepareData workerPrepareData;
    private Configuration configuration;
    private WorkspaceRepository workspaceRepository;
    private GraphRepository graphRepository;

    public VerifyResults verify() {
        return new VerifyResults();
    }

    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        this.workerPrepareData = workerPrepareData;
    }

    protected void applyTermMentionFilters(Vertex outVertex, Iterable<Vertex> termMentions) {
        for (TermMentionFilter termMentionFilter : this.workerPrepareData.getTermMentionFilters()) {
            try {
                termMentionFilter.apply(outVertex, termMentions, this.workerPrepareData.getAuthorizations());
            } catch (Exception e) {
                LOGGER.error("Could not apply term mention filter", e);
            }
        }
        getGraph().flush();
    }

    protected void pushTextUpdated(DataWorkerData data) {
        if (data == null || data.getElement() == null) {
            return;
        }
        getWebQueueRepository().pushTextUpdated(data.getElement().getId(), data.getPriority());
    }

    public abstract boolean isHandled(Element element, Property property);

    public boolean isDeleteHandled(Element element, Property property) {
        return false;
    }

    public boolean isHiddenHandled(Element element, Property property) {
        return false;
    }

    public boolean isUnhiddenHandled(Element element, Property property) {
        return false;
    }

    public abstract void execute(InputStream in, DataWorkerData data) throws Exception;

    public Element refresh(Element element) {
        if(element == null)
            return null;

        if(element instanceof Vertex)
            return getGraph().getVertex(element.getId(), getAuthorizations());
        else if(element instanceof Edge)
            return getGraph().getEdge(element.getId(), getAuthorizations());
        else
            throw new BcException("Asking to refresh an element of unknown type: "+element.getClass());
    }

    public boolean isLocalFileRequired() {
        return false;
    }

    protected User getUser() {
        return this.workerPrepareData.getUser();
    }

    public Authorizations getAuthorizations() {
        return this.workerPrepareData.getAuthorizations();
    }

    @Inject
    public final void setGraph(Graph graph) {
        this.graph = graph;
    }

    protected Graph getGraph() {
        return graph;
    }

    @Inject
    public final void setWorkQueueRepository(WorkQueueRepository workQueueRepository) {
        this.workQueueRepository = workQueueRepository;
    }

    @Inject
    public void setWebQueueRepository(WebQueueRepository webQueueRepository) {
        this.webQueueRepository = webQueueRepository;
    }

    @Inject
    public final void setWorkspaceRepository(WorkspaceRepository workspaceRepository) {
        this.workspaceRepository = workspaceRepository;
    }

    protected WorkspaceRepository getWorkspaceRepository() {
        return workspaceRepository;
    }

    protected WorkQueueRepository getWorkQueueRepository() {
        return workQueueRepository;
    }

    public WebQueueRepository getWebQueueRepository() {
        return webQueueRepository;
    }

    protected SchemaRepository getSchemaRepository() {
        return schemaRepository;
    }

    @Inject
    public final void setSchemaRepository(SchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
    }

    protected Configuration getConfiguration() {
        return configuration;
    }

    @Inject
    public final void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    protected VisibilityTranslator getVisibilityTranslator() {
        return visibilityTranslator;
    }

    @Inject
    public final void setVisibilityTranslator(VisibilityTranslator visibilityTranslator) {
        this.visibilityTranslator = visibilityTranslator;
    }

    @Inject
    public final void setGraphAuthorizationRepository(GraphAuthorizationRepository graphAuthorizationRepository) {
        this.graphAuthorizationRepository = graphAuthorizationRepository;
    }

    protected GraphAuthorizationRepository getGraphAuthorizationRepository() {
        return graphAuthorizationRepository;
    }

    public GraphRepository getGraphRepository() {
        return graphRepository;
    }

    @Inject
    public final void setGraphRepository(GraphRepository graphRepository) {
        this.graphRepository = graphRepository;
    }

    /**
     * Determines if this is a property that should be analyzed by text processing tools.
     */
    protected boolean isTextProperty(Property property) {
        if (property == null) {
            return false;
        }

        if (property.getName().equals(BcSchema.RAW.getPropertyName())) {
            return false;
        }

        Value mimeType = property.getMetadata().getValue(BcSchema.MIME_TYPE.getPropertyName());
        return !(mimeType.eq(Values.NO_VALUE) || !((TextValue)mimeType).startsWith(Values.stringValue("text")));
    }

    protected static boolean isVertex(Element element) {
        if (!(element instanceof Vertex)) {
            return false;
        }
        return true;
    }

    protected static boolean isConceptType(Element element, String conceptType) {
        if (element instanceof Vertex) {
            Vertex vertex = (Vertex) element;
            String elementConceptType = vertex.getConceptType();
            if (elementConceptType == null) {
                return false;
            }

            if (!elementConceptType.equals(conceptType)) {
                return false;
            }

            return true;
        }

        return false;
    }

    protected void addVideoTranscriptAsTextPropertiesToMutation(ExistingElementMutation<Vertex> mutation, String propertyKey, VideoTranscript videoTranscript, Metadata metadata, Visibility visibility) {
        BcSchema.MIME_TYPE_METADATA.setMetadata(metadata, "text/plain", getVisibilityTranslator().getDefaultVisibility());
        for (VideoTranscript.TimedText entry : videoTranscript.getEntries()) {
            String textPropertyKey = getVideoTranscriptTimedTextPropertyKey(propertyKey, entry);
            StreamingPropertyValue value = new DefaultStreamingPropertyValue(new ByteArrayInputStream(entry.getText().getBytes()), TextValue.class);
            BcSchema.TEXT.addPropertyValue(mutation, textPropertyKey, value, metadata, visibility);
        }
    }

    protected void pushVideoTranscriptTextPropertiesOnWorkQueue(Element element, String propertyKey, VideoTranscript videoTranscript, Priority priority) {
        for (VideoTranscript.TimedText entry : videoTranscript.getEntries()) {
            String textPropertyKey = getVideoTranscriptTimedTextPropertyKey(propertyKey, entry);

            if (getWebQueueRepository().shouldBroadcast(priority)) {
                getWebQueueRepository().broadcastPropertyChange(element, textPropertyKey, BcSchema.TEXT.getPropertyName(), null);
            }

            getWorkQueueRepository().pushGraphPropertyQueue(
                    element,
                    textPropertyKey,
                    BcSchema.TEXT.getPropertyName(),
                    null,
                    null,
                    priority,
                    ElementOrPropertyStatus.UPDATE,
                    null
            );
        }
    }

    private String getVideoTranscriptTimedTextPropertyKey(String propertyKey, VideoTranscript.TimedText entry) {
        String startTime = String.format("%08d", Math.max(0L, entry.getTime().getStart()));
        String endTime = String.format("%08d", Math.max(0L, entry.getTime().getEnd()));
        return propertyKey + RowKeyHelper.FIELD_SEPARATOR + MediaBcSchema.VIDEO_FRAME.getPropertyName() + RowKeyHelper.FIELD_SEPARATOR + startTime + RowKeyHelper.FIELD_SEPARATOR + endTime;
    }

    protected void addVertexToWorkspaceIfNeeded(DataWorkerData data, Vertex vertex) {
        if (data.getWorkspaceId() == null) {
            return;
        }
        graph.flush();
        getWorkspaceRepository().updateEntityOnWorkspace(data.getWorkspaceId(), vertex.getId(), getUser());
    }

    protected void pushChangedPropertiesOnWorkQueue(DataWorkerData data, List<BcPropertyUpdate> changedProperties) {
        getWebQueueRepository().broadcastPropertiesChange(
                data.getElement(),
                changedProperties,
                data.getWorkspaceId(),
                data.getPriority()
        );
        getWorkQueueRepository().pushGraphPropertyQueue(
                data.getElement(),
                changedProperties,
                data.getWorkspaceId(),
                data.getVisibilitySource(),
                data.getPriority()
        );
    }

    public boolean systemPlugin() {
        return false;
    }
}
