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
package com.mware.core.ingest.structured;

import com.google.inject.Inject;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.structured.mapping.ParseMapping;
import com.mware.core.ingest.structured.model.StructuredIngestParser;
import com.mware.core.ingest.structured.model.StructuredIngestParserFactory;
import com.mware.core.ingest.structured.model.StructuredIngestQueueItem;
import com.mware.core.ingest.structured.util.GraphBuilderParserHandler;
import com.mware.core.ingest.structured.util.ProgressReporter;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.longRunningProcess.LongRunningProcessWorker;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workspace.WorkspaceHelper;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.ClientApiConverter;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.Vertex;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;
import org.json.JSONObject;

import java.io.InputStream;
import java.text.NumberFormat;

@Name("Structured Import")
@Description("Extracts structured data from csv, and excel")
public class StructuredIngestProcessWorker extends LongRunningProcessWorker {
    public static final String TYPE = "structured-ingest";
    private SchemaRepository schemaRepository;
    private VisibilityTranslator visibilityTranslator;
    private PrivilegeRepository privilegeRepository;
    private WorkspaceHelper workspaceHelper;
    private WorkspaceRepository workspaceRepository;
    private UserRepository userRepository;
    private Configuration configuration;
    private StructuredIngestParserFactory structuredIngestParserFactory;
    private Graph graph;
    private LongRunningProcessRepository longRunningProcessRepository;

    @Override
    public boolean isHandled(JSONObject longRunningProcessQueueItem) {
        return TYPE.equals(longRunningProcessQueueItem.getString("type"));
    }

    @Override
    protected void processInternal(final JSONObject longRunningProcessQueueItem) {
        StructuredIngestQueueItem structuredIngestQueueItem = ClientApiConverter.toClientApi(longRunningProcessQueueItem.toString(), StructuredIngestQueueItem.class);
        ParseMapping parseMapping = new ParseMapping(schemaRepository, visibilityTranslator, structuredIngestQueueItem.getWorkspaceId(), structuredIngestQueueItem.getMapping());
        Authorizations authorizations = graph.createAuthorizations(structuredIngestQueueItem.getAuthorizations());
        Vertex vertex = graph.getVertex(structuredIngestQueueItem.getVertexId(), authorizations);
        User user = userRepository.findById(structuredIngestQueueItem.getUserId());
        StreamingPropertyValue rawPropertyValue = BcSchema.RAW.getPropertyValue(vertex);
        NumberFormat numberFormat = NumberFormat.getIntegerInstance();

        ProgressReporter reporter = new ProgressReporter() {
            public void finishedRow(long row, long totalRows) {
                if (totalRows != -1) {
                    if(row % 100 == 0 || row == totalRows) {
                        longRunningProcessRepository.reportProgress(
                                longRunningProcessQueueItem,
                                ((float) row) / ((float) totalRows),
                                "Row " + numberFormat.format(row) + " of " + numberFormat.format(totalRows));
                    }
                }
            }
        };
        GraphBuilderParserHandler parserHandler = new GraphBuilderParserHandler(
                graph,
                user,
                visibilityTranslator,
                privilegeRepository,
                authorizations,
                workspaceRepository,
                workspaceHelper,
                structuredIngestQueueItem.getWorkspaceId(),
                structuredIngestQueueItem.isPublish(),
                vertex,
                parseMapping,
                reporter,
                schemaRepository);


        longRunningProcessRepository.reportProgress(longRunningProcessQueueItem, 0, "Deleting previous imports");
        parserHandler.cleanUpExistingImport();

        parserHandler.dryRun = false;
        parserHandler.reset();
        try {
            parse(vertex, rawPropertyValue, parserHandler, structuredIngestQueueItem, user);
        } catch (Exception e) {
            throw new BcException("Unable to ingest vertex: " + vertex, e);
        }
    }

    private void parse(Vertex vertex, StreamingPropertyValue rawPropertyValue, GraphBuilderParserHandler parserHandler, StructuredIngestQueueItem item, User user) throws Exception {
        TextValue mimeType = (TextValue) vertex.getPropertyValue(BcSchema.MIME_TYPE.getPropertyName());
        if (mimeType == null) {
            throw new BcException("No mimeType property found for vertex");
        }

        StructuredIngestParser structuredIngestParser = structuredIngestParserFactory.getParser(mimeType.stringValue());
        if (structuredIngestParser == null) {
            throw new BcException("No parser registered for mimeType: " + mimeType);
        }

        try (InputStream in = rawPropertyValue.getInputStream()) {
            structuredIngestParser.ingest(in, item.getParseOptions(), parserHandler, user);
        }
    }

    @Inject
    public void setPrivilegeRepository(PrivilegeRepository privilegeRepository) {
        this.privilegeRepository = privilegeRepository;
    }

    @Inject
    public void setSchemaRepository(SchemaRepository ontologyRepository) {
        this.schemaRepository = ontologyRepository;
    }

    @Inject
    public void setVisibilityTranslator(VisibilityTranslator visibilityTranslator) {
        this.visibilityTranslator = visibilityTranslator;
    }

    @Inject
    public void setWorkspaceHelper(WorkspaceHelper workspaceHelper) {
        this.workspaceHelper = workspaceHelper;
    }

    @Inject
    public void setUserRepository(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Inject
    public void setWorkspaceRepository(WorkspaceRepository workspaceRepository) {
        this.workspaceRepository = workspaceRepository;
    }

    @Inject
    public void setStructuredIngestParserFactory(StructuredIngestParserFactory structuredIngestParserFactory) {
        this.structuredIngestParserFactory = structuredIngestParserFactory;
    }

    @Inject
    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    @Inject
    public void setLongRunningProcessRepository(LongRunningProcessRepository longRunningProcessRepository) {
        this.longRunningProcessRepository = longRunningProcessRepository;
    }

    @Inject
    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}


