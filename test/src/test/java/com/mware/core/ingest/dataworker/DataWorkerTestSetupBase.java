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

import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.InMemoryGraphAuthorizationRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Visibility;
import com.mware.ge.inmemory.InMemoryGraph;
import org.junit.Before;
import org.mockito.Mock;

import java.util.*;

import static com.mware.core.model.schema.SchemaRepository.PUBLIC;
import static org.mockito.Mockito.when;

/**
 * This base class provides a common test setup for unit tests of DataWorker subclasses.
 * Both Mockito and in-memory implementations are used to supply dependent objects to the DataWorker.
 * <p/>
 * TODO: There are a number of DataWorker unit tests with copied/pasted setup that should extend this base class.
 *
 * @deprecated use {@link DataWorkerTestBase} instead
 */
@Deprecated
public abstract class DataWorkerTestSetupBase {
    protected static final String WORKSPACE_ID = "TEST_WORKSPACE";
    protected static final String VISIBILITY_SOURCE = "TEST_VISIBILITY_SOURCE";
    protected Authorizations termMentionAuthorizations;

    @Mock
    protected User user;
    @Mock
    protected SchemaRepository schemaRepository;
    @Mock
    protected WorkspaceRepository workspaceRepository;
    @Mock
    protected WorkQueueRepository workQueueRepository;
    protected GraphAuthorizationRepository graphAuthorizationRepository;
    protected TermMentionRepository termMentionRepository;
    protected Map<String, String> configuration = new HashMap<>();
    protected Authorizations authorizations;
    protected InMemoryGraph graph;
    protected Visibility visibility;
    protected VisibilityJson visibilityJson;
    protected VisibilityTranslator visibilityTranslator;
    protected DataWorker worker; // test subject

    @Before
    public void setup() throws Exception {
        configuration.put("ontology.intent.concept.person", "test#person");
        configuration.put("ontology.intent.concept.location", "test#location");
        configuration.put("ontology.intent.concept.organization", "test#organization");
        configuration.put("ontology.intent.concept.phoneNumber", "test#phoneNumber");
        configuration.put("ontology.intent.relationship.artifactHasEntity", "test#artifactHasEntity");

        when(schemaRepository.getRequiredConceptNameByIntent("location", PUBLIC)).thenReturn("test#location");
        when(schemaRepository.getRequiredConceptNameByIntent("organization", PUBLIC)).thenReturn("test#organization");
        when(schemaRepository.getRequiredConceptNameByIntent("person", PUBLIC)).thenReturn("test#person");
        when(schemaRepository.getRequiredConceptNameByIntent("phoneNumber", PUBLIC)).thenReturn("test#phoneNumber");
        when(schemaRepository.getRequiredRelationshipNameByIntent("artifactHasEntity", PUBLIC)).thenReturn("test#artifactHasEntity");

        when(user.getUserId()).thenReturn("USER123");

        List<TermMentionFilter> termMentionFilters = new ArrayList<>();
        authorizations = new Authorizations(VISIBILITY_SOURCE);
        configuration.putAll(getAdditionalConfiguration());
        DataWorkerPrepareData workerPrepareData = new DataWorkerPrepareData(
                configuration,
                termMentionFilters,
                user,
                authorizations,
                null
        );
        graph = InMemoryGraph.create();
        termMentionAuthorizations = graph.createAuthorizations(TermMentionRepository.VISIBILITY_STRING, VISIBILITY_SOURCE, WORKSPACE_ID);
        visibility = new Visibility(VISIBILITY_SOURCE);
        visibilityJson = new VisibilityJson();
        visibilityJson.setSource(VISIBILITY_SOURCE);
        visibilityJson.addWorkspace(WORKSPACE_ID);
        visibilityTranslator = new DirectVisibilityTranslator();
        graphAuthorizationRepository = new InMemoryGraphAuthorizationRepository();
        termMentionRepository = new TermMentionRepository(graph, graphAuthorizationRepository);

        worker = createGraphPropertyWorker();
        worker.setVisibilityTranslator(visibilityTranslator);
        worker.setConfiguration(new HashMapConfigurationLoader(configuration).createConfiguration());
        worker.setSchemaRepository(schemaRepository);
        worker.setWorkspaceRepository(workspaceRepository);
        worker.setWorkQueueRepository(workQueueRepository);
        worker.prepare(workerPrepareData);
        worker.setGraph(graph);
    }

    protected abstract DataWorker createGraphPropertyWorker();

    protected Map<String, String> getAdditionalConfiguration() {
        return Collections.emptyMap();
    }
}
