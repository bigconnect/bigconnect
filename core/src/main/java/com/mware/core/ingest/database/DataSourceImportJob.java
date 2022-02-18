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
package com.mware.core.ingest.database;

import com.google.inject.Inject;
import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.ingest.structured.mapping.ParseMapping;
import com.mware.core.ingest.structured.util.ProgressReporter;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.Relationship;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.Graph;
import com.mware.ge.mutation.ElementMutation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class DataSourceImportJob {
    BcLogger logger = BcLoggerFactory.getLogger(DataSourceImportJob.class);

    private final Graph graph;
    private final UserRepository userRepository;
    private final DataConnectionRepository dataConnectionRepository;
    private final VisibilityTranslator visibilityTranslator;
    private final PrivilegeRepository privilegeRepository;
    private final SchemaRepository schemaRepository;
    private final WorkQueueRepository workQueueRepository;
    private final DataSourceManager dataSourceManager;

    boolean shouldRun = false;
    private ClientApiDataSource params;
    private ProgressReporter progressReporter;
    private Authorizations authorizations;
    private User user;

    @Inject
    public DataSourceImportJob(
            Graph graph,
            UserRepository userRepository,
            DataConnectionRepository dataConnectionRepository,
            VisibilityTranslator visibilityTranslator,
            PrivilegeRepository privilegeRepository,
            SchemaRepository schemaRepository,
            WorkQueueRepository workQueueRepository,
            DataSourceManager dataSourceManager
    ) {
        this.graph = graph;
        this.userRepository = userRepository;
        this.dataConnectionRepository = dataConnectionRepository;
        this.visibilityTranslator = visibilityTranslator;
        this.privilegeRepository = privilegeRepository;
        this.schemaRepository = schemaRepository;
        this.workQueueRepository = workQueueRepository;
        this.dataSourceManager = dataSourceManager;
    }

    public void prepare(ClientApiDataSource params, ProgressReporter progressReporter) {
        this.params = params;
        this.progressReporter = progressReporter;

        this.authorizations = graph.createAuthorizations(params.getAuthorizations());
        this.user = userRepository.findById(params.getUserId());
    }

    /**
     * @throws Exception
     */
    public void run() throws Exception {
        logger.info("Begin data load [with dictionary import: " + params.isImportEntitiesToDictionaries() + "]");
        shouldRun = true;

        DataConnection dataConnection = dataConnectionRepository.findDcById(params.getDcId());
        List<ElementMutation<? extends Element>> batchElementBuilders = new ArrayList<>();
        try (Connection sqlConn = dataSourceManager.getSqlConnection(dataConnection)){
            long totalRows = getTotalRows(sqlConn, params.getSqlSelect());

            if (totalRows == 0) {
                return;
            }

            publishRequiredOntologyObjects();

            ParseMapping parseMapping = ParseMapping.fromDataSourceImport(schemaRepository, visibilityTranslator, params);

            DataLoadGraphBuilder builder = new DataLoadGraphBuilder(
                    graph,
                    user,
                    privilegeRepository,
                    authorizations,
                    visibilityTranslator,
                    params,
                    parseMapping,
                    progressReporter,
                    userRepository,
                    totalRows);

            PreparedStatement stmt = sqlConn.prepareStatement(params.getSqlSelect());
            ResultSet rs = stmt.executeQuery();

            long currentRow = 1;
            while(rs.next()) {
                if(!shouldRun || !builder.addRow(rs, currentRow++,
                        params.isImportEntitiesToDictionaries(), batchElementBuilders)) {
                    break;
                }

                if (batchElementBuilders.size() > params.getCommitBatchSize()) {
                    flushData(batchElementBuilders);
                }
            }

            // flush remaining element builders
            flushData(batchElementBuilders);
        }

        dataConnectionRepository.setImportRunning(params.getDsId(), false);
        dataConnectionRepository.setLastImportDate(params.getDsId(), ZonedDateTime.now());
        logger.info("End data load");
    }

    private void publishRequiredOntologyObjects() {
        params.getEntityMappings().forEach(m -> {
            if(m.getColConcept() != null && m.getColProperty() != null) {
                Concept concept = schemaRepository.getConceptByName(m.getColConcept(), params.getWorkspaceId());
                if(concept != null && concept.getSandboxStatus() != SandboxStatus.PUBLIC)
                    schemaRepository.publishConcept(concept, user, params.getWorkspaceId());

                SchemaProperty property = schemaRepository.getPropertyByName(m.getColProperty(), params.getWorkspaceId());
                if(property != null && property.getSandboxStatus() != SandboxStatus.PUBLIC) {
                    schemaRepository.publishProperty(property, user, params.getWorkspaceId());
                }
            }
        });

        params.getRelMappings().forEach(m -> {
            if(m.getRel() != null) {
                Relationship relationship = schemaRepository.getRelationshipByName(m.getRel(), params.getWorkspaceId());
                if(relationship != null && relationship.getSandboxStatus() != SandboxStatus.PUBLIC)
                    schemaRepository.publishRelationship(relationship, user, params.getWorkspaceId());
            }
        });
    }

    public void stop() {
        shouldRun = false;
        dataConnectionRepository.setImportRunning(params.getDsId(), false);
        dataConnectionRepository.setLastImportDate(params.getDsId(), ZonedDateTime.now());
    }

    private void flushData(List<ElementMutation<? extends Element>> batchElementBuilders) {
        workQueueRepository.pushMultipleElementOnDwQueue(
                graph.saveElementMutations(batchElementBuilders, authorizations),
                null,
                null,
                null,
                null,
                Priority.LOW,
                ElementOrPropertyStatus.UPDATE,
                null
        );
        graph.flush();
        batchElementBuilders.clear();
    }

    private long getTotalRows(Connection sqlConn, String sqlSelect) throws Exception {
        try (Statement stmt = sqlConn.createStatement()) {
            try {
                String countSql = "select count(*) from ("+sqlSelect+") cnt";
                ResultSet rs = stmt.executeQuery(countSql);

                if(rs.next()) {
                    return rs.getLong(1);
                }

                return 0;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return 0;
    }
}
