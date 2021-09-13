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

import com.google.inject.Injector;
import com.mware.core.config.Configuration;
import com.mware.core.config.ConfigurationLoader;
import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.InMemoryUser;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.*;
import com.mware.core.model.worker.InMemoryDataWorkerTestBase;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.*;
import com.mware.ge.id.IdGenerator;
import com.mware.ge.id.QueueIdGenerator;
import com.mware.ge.inmemory.InMemoryGraph;
import com.mware.ge.inmemory.InMemoryGraphConfiguration;
import com.mware.ge.search.DefaultSearchIndex;
import com.mware.ge.search.SearchIndex;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.TextValue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mware.core.model.workQueue.WorkQueueRepository.DW_DEFAULT_QUEUE_NAME;
import static org.mockito.Mockito.mock;

/**
 * @deprecated Use {@link InMemoryDataWorkerTestBase}
 */
@Deprecated
public abstract class DataWorkerTestBase {
    private InMemoryGraph graph;
    private IdGenerator graphIdGenerator;
    private SearchIndex graphSearchIndex;
    private HashMap<String, String> configurationMap;
    private DataWorkerPrepareData graphPropertyWorkerPrepareData;
    private User user;
    private WorkQueueRepository workQueueRepository;
    private WebQueueRepository webQueueRepository;
    private GraphRepository graphRepository;
    private TermMentionRepository termMentionRepository;
    private VisibilityTranslator visibilityTranslator = new VisibilityTranslator();

    @Mock
    protected SchemaRepository schemaRepository;

    @Mock
    protected UserRepository userRepository;

    @Mock
    protected AuthorizationRepository authorizationRepository;

    @Mock
    protected WorkspaceRepository workspaceRepository;

    protected DataWorkerTestBase() {

    }

    @Before
    public final void clearGraph() {
        if (graph != null) {
            graph.shutdown();
            graph = null;
        }
        graphIdGenerator = null;
        graphSearchIndex = null;
        configurationMap = null;
        graphPropertyWorkerPrepareData = null;
        user = null;
        workQueueRepository = null;
        webQueueRepository = null;
        System.setProperty(ConfigurationLoader.ENV_CONFIGURATION_LOADER, HashMapConfigurationLoader.class.getName());
        InMemoryWorkQueueRepository.clearQueue();
    }

    @After
    public final void after() {
        clearGraph();
    }

    protected DataWorkerPrepareData getWorkerPrepareData() {
        return getWorkerPrepareData(null, null, null, null, null);
    }

    protected DataWorkerPrepareData getWorkerPrepareData(Map configuration, List<TermMentionFilter> termMentionFilters, User user, Authorizations authorizations, Injector injector) {
        if (graphPropertyWorkerPrepareData == null) {
            if (configuration == null) {
                configuration = getConfigurationMap();
            }
            if (termMentionFilters == null) {
                termMentionFilters = new ArrayList<>();
            }
            if (user == null) {
                user = getUser();
            }
            if (authorizations == null) {
                authorizations = getGraphAuthorizations(BcVisibility.SUPER_USER_VISIBILITY_STRING);
            }
            graphPropertyWorkerPrepareData = new DataWorkerPrepareData(configuration, termMentionFilters, user, authorizations, injector);
        }
        return graphPropertyWorkerPrepareData;
    }

    protected User getUser() {
        if (user == null) {
            user = new InMemoryUser("test", "Test User", "test@example.org", null);
        }
        return user;
    }

    protected Graph getGraph() {
        if (graph == null) {
            Map graphConfiguration = getConfigurationMap();
            InMemoryGraphConfiguration inMemoryGraphConfiguration = new InMemoryGraphConfiguration(graphConfiguration);
            graph = InMemoryGraph.create(inMemoryGraphConfiguration, getGraphIdGenerator(), getGraphSearchIndex(inMemoryGraphConfiguration));

            graph.defineProperty(BcSchema.MODIFIED_BY.getPropertyName()).dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();
            graph.defineProperty(BcSchema.MODIFIED_DATE.getPropertyName()).dataType(DateTimeValue.class).define();
            graph.defineProperty(BcSchema.VISIBILITY_JSON.getPropertyName()).dataType(TextValue.class).define();
            graph.defineProperty(BcSchema.TEXT.getPropertyName()).dataType(TextValue.class).textIndexHint(TextIndexHint.FULL_TEXT).define();
        }
        return graph;
    }

    protected IdGenerator getGraphIdGenerator() {
        if (graphIdGenerator == null) {
            graphIdGenerator = new QueueIdGenerator();
        }
        return graphIdGenerator;
    }

    protected SearchIndex getGraphSearchIndex(GraphConfiguration inMemoryGraphConfiguration) {
        if (graphSearchIndex == null) {
            graphSearchIndex = new DefaultSearchIndex(inMemoryGraphConfiguration);
        }
        return graphSearchIndex;
    }

    protected Map getConfigurationMap() {
        if (configurationMap == null) {
            configurationMap = new HashMap<>();
            configurationMap.put("ontology.intent.concept.location", "test#location");
            configurationMap.put("ontology.intent.concept.organization", "test#organization");
            configurationMap.put("ontology.intent.concept.person", "test#person");
        }
        return configurationMap;
    }

    protected Authorizations getGraphAuthorizations(String... authorizations) {
        return getGraph().createAuthorizations(authorizations);
    }

    protected void setSchemaRepository(SchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
    }

    protected byte[] getResourceAsByteArray(Class sourceClass, String resourceName) {
        try {
            InputStream in = sourceClass.getResourceAsStream(resourceName);
            if (in == null) {
                throw new IOException("Could not find resource: " + resourceName);
            }
            return IOUtils.toByteArray(in);
        } catch (IOException e) {
            throw new BcException("Could not load resource. " + sourceClass.getName() + " at " + resourceName, e);
        }
    }

    /**
     * @deprecated {@link #run(DataWorker, DataWorkerPrepareData, Element, Property, InputStream, String, ElementOrPropertyStatus, String)} will prepare the worker
     */
    @Deprecated
    protected void prepare(DataWorker gpw) throws Exception {
        gpw.setSchemaRepository(schemaRepository);
        gpw.setWorkspaceRepository(workspaceRepository);
        gpw.setVisibilityTranslator(getVisibilityTranslator());
        gpw.setGraph(getGraph());
        gpw.setWorkQueueRepository(getWorkQueueRepository());
        gpw.setWebQueueRepository(getWebQueueRepository());
        gpw.setGraphRepository(getGraphRepository());
        gpw.prepare(getWorkerPrepareData());
    }

    protected void run(DataWorker gpw, DataWorkerPrepareData workerPrepareData, Element e) {
        run(gpw, workerPrepareData, e, null);
    }

    protected void run(
            DataWorker gpw,
            DataWorkerPrepareData workerPrepareData,
            Element e,
            String workspaceId
    ) {
        String visibilitySource = getVisibilitySource(e);
        run(gpw, workerPrepareData, e, null, null, workspaceId, null, visibilitySource);
        for (Property property : e.getProperties()) {
            InputStream in = null;
            if (property.getValue() instanceof StreamingPropertyValue) {
                StreamingPropertyValue spv = (StreamingPropertyValue) property.getValue();
                in = spv.getInputStream();
            }
            run(gpw, workerPrepareData, e, property, in, workspaceId, null, visibilitySource);
        }
    }

    private String getVisibilitySource(Element e) {
        String visibilitySource = null;
        if (e != null) {
            VisibilityJson visibilitySourceJson = BcSchema.VISIBILITY_JSON.getPropertyValue(e, null);
            if (visibilitySourceJson != null) {
                visibilitySource = visibilitySourceJson.getSource();
            }
        }
        return visibilitySource;
    }

    protected boolean run(DataWorker gpw, DataWorkerPrepareData workerPrepareData, Element e, Property prop, InputStream in) {
        String visibilitySource = getVisibilitySource(e);
        return run(gpw, workerPrepareData, e, prop, in, null, null, visibilitySource);
    }

    protected boolean run(DataWorker gpw, DataWorkerPrepareData workerPrepareData, Element e, Property prop, InputStream in, String workspaceId, ElementOrPropertyStatus status) {
        String visibilitySource = getVisibilitySource(e);
        return run(gpw, workerPrepareData, e, prop, in, workspaceId, status, visibilitySource);
    }

    protected boolean run(
            DataWorker gpw,
            DataWorkerPrepareData workerPrepareData,
            Element e,
            Property prop,
            InputStream in,
            String workspaceId,
            ElementOrPropertyStatus status,
            String visibilitySource
    ) {
        try {
            gpw.setSchemaRepository(schemaRepository);
            gpw.setWorkspaceRepository(workspaceRepository);
            gpw.setConfiguration(getConfiguration());
            gpw.setSchemaRepository(schemaRepository);
            gpw.setGraph(getGraph());
            gpw.setVisibilityTranslator(getVisibilityTranslator());
            gpw.setWorkQueueRepository(getWorkQueueRepository());
            gpw.setGraphRepository(getGraphRepository());
            gpw.prepare(workerPrepareData);
        } catch (Exception ex) {
            throw new BcException("Failed to prepare: " + gpw.getClass().getName(), ex);
        }

        try {
            if (!(status == ElementOrPropertyStatus.HIDDEN && gpw.isHiddenHandled(e, prop))
                    && !(status == ElementOrPropertyStatus.DELETION && gpw.isDeleteHandled(e, prop))
                    && !gpw.isHandled(e, prop)) {
                return false;
            }
        } catch (Exception ex) {
            throw new BcException("Failed to isHandled: " + gpw.getClass().getName(), ex);
        }

        try {
            DataWorkerData executeData = new DataWorkerData(
                    visibilityTranslator,
                    e,
                    prop,
                    workspaceId,
                    visibilitySource,
                    Priority.NORMAL,
                    false,
                    (prop == null ? e.getTimestamp() : prop.getTimestamp()) - 1,
                    status
            );
            if (gpw.isLocalFileRequired() && executeData.getLocalFile() == null && in != null) {
                byte[] data = IOUtils.toByteArray(in);
                File tempFile = File.createTempFile("bcTest", "data");
                FileUtils.writeByteArrayToFile(tempFile, data);
                executeData.setLocalFile(tempFile);
                in = new ByteArrayInputStream(data);
            }
            gpw.execute(in, executeData);
        } catch (Exception ex) {
            throw new BcException("Failed to execute: " + gpw.getClass().getName(), ex);
        }
        return true;
    }

    protected WorkQueueRepository getWorkQueueRepository() {
        if (workQueueRepository == null) {
            workQueueRepository = new InMemoryWorkQueueRepository(
                    getGraph(),
                    getConfiguration()
            );
        }
        return workQueueRepository;
    }

    protected WebQueueRepository getWebQueueRepository() {
        if (webQueueRepository == null) {
            webQueueRepository = new InMemoryWebQueueRepository();
            webQueueRepository.setUserRepository(userRepository);
            webQueueRepository.setAuthorizationRepository(authorizationRepository);
            webQueueRepository.setWorkspaceRepository(workspaceRepository);
        }
        return webQueueRepository;
    }

    protected TermMentionRepository getTermMentionRepository() {
        if (termMentionRepository == null) {
            termMentionRepository = mock(TermMentionRepository.class);
        }
        return termMentionRepository;
    }

    protected GraphRepository getGraphRepository() {
        if (graphRepository == null) {
            graphRepository = new GraphRepository(
                    getGraph(),
                    getTermMentionRepository(),
                    getWorkQueueRepository(),
                    getWebQueueRepository(),
                    getConfiguration()
            );
        }
        return graphRepository;
    }

    protected List<byte[]> getGraphPropertyQueue() {
        return InMemoryWorkQueueRepository.getQueue(
                getConfiguration().get(Configuration.DW_QUEUE_NAME, DW_DEFAULT_QUEUE_NAME));
    }

    protected VisibilityTranslator getVisibilityTranslator() {
        return visibilityTranslator;
    }

    protected Configuration getConfiguration() {
        return new HashMapConfigurationLoader(getConfigurationMap()).createConfiguration();
    }
}
