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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.database.model.ClientApiDataConnection;
import com.mware.core.ingest.database.model.ClientApiDataConnections;
import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.model.clientapi.util.ClientApiConverter;
import com.mware.core.model.clientapi.util.StringUtils;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.security.BcVisibility;
import com.mware.core.trace.Traced;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.mutation.ExistingElementMutation;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.values.storable.Values;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mware.ge.util.IterableUtils.singleOrDefault;
import static com.mware.ge.util.StreamUtils.stream;

public class GeDataConnectionRepository implements DataConnectionRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(GeDataConnectionRepository.class);
    public static final String GRAPH_DC_ID_PREFIX = "DC_";
    public static final String GRAPH_DS_ID_PREFIX = "DS_";
    public static final BcVisibility VISIBILITY = new BcVisibility(VISIBILITY_STRING);

    private final UserRepository userRepository;
    private final Graph graph;
    private final AuthorizationRepository authorizationRepository;
    private final Authorizations authorizations;
    private final DataSourceManager dataSourceManager;
    private final Configuration configuration;

    private final Cache<String, Vertex> dcVertexCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();
    private final Cache<String, Vertex> dsVertexCache = CacheBuilder.newBuilder()
            .expireAfterWrite(15, TimeUnit.SECONDS)
            .build();

    @Inject
    public GeDataConnectionRepository(
            GraphAuthorizationRepository graphAuthorizationRepository,
            UserRepository userRepository,
            Graph graph,
            AuthorizationRepository authorizationRepository,
            Configuration configuration
    ) {
        this.userRepository = userRepository;
        this.graph = graph;
        this.authorizationRepository = authorizationRepository;
        this.configuration = configuration;

        graphAuthorizationRepository.addAuthorizationToGraph(VISIBILITY_STRING);

        Set<String> authorizationsSet = new HashSet<>();
        authorizationsSet.add(VISIBILITY_STRING);
        authorizationsSet.add(UserRepository.VISIBILITY_STRING);
        authorizationsSet.add(BcVisibility.SUPER_USER_VISIBILITY_STRING);
        this.authorizations = graph.createAuthorizations(authorizationsSet);

        this.dataSourceManager = InjectHelper.getInstance(DataSourceManager.class);
    }

    @Override
    public Iterable<DataConnection> getAllDataConnections() {
        try (QueryResultsIterable<Vertex> vertices = graph.query(authorizations)
                .hasConceptType(DataConnectionSchema.DATA_CONNECTION_CONCEPT_NAME)
                .vertices()) {
            return new ConvertingIterable<Vertex, DataConnection>(vertices) {
                @Override
                protected DataConnection convert(Vertex vertex) {
                    return createDataConnectionFromVertex(vertex);
                }
            };
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public DataConnection findDcById(String dataConnectionId) {
        return createDataConnectionFromVertex(findByIdDataConnectionVertex(dataConnectionId, graph.getDefaultFetchHints()));
    }

    @Override
    public DataConnection findByName(String dataConnectionName) {
        Iterable<Vertex> vertices = graph.query(authorizations)
                .has(DataConnectionSchema.DC_NAME.getPropertyName(), Values.stringValue(dataConnectionName))
                .hasConceptType(DataConnectionSchema.DATA_CONNECTION_CONCEPT_NAME)
                .vertices();
        Vertex vertex = singleOrDefault(vertices, null);
        if (vertex == null) {
            return null;
        }
        dcVertexCache.put(vertex.getId(), vertex);
        return createDataConnectionFromVertex(vertex);
    }

    public boolean checkDataConnection(DataConnection dc) throws BcException {
        boolean returnVal = false;
        Connection connection = this.getDataSourceManager().getSqlConnection(dc);
        if (connection != null) {
            returnVal = true;
        }
        return returnVal;
    }

    @Override
    public DataConnection createDataConnection(String name, String description, String driverClass, String driverProperties, String jdbcUrl, String username, String password) {
        name = StringUtils.trimIfNull(name);
        description = StringUtils.trimIfNull(description);

        String id = GRAPH_DC_ID_PREFIX + graph.getIdGenerator().nextId();
        VertexBuilder builder = graph.prepareVertex(id, VISIBILITY.getVisibility(), DataConnectionSchema.DATA_CONNECTION_CONCEPT_NAME);
        DataConnectionSchema.DC_NAME.setProperty(builder, name, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_DESCRIPTION.setProperty(builder, description, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_DRIVER_CLASS.setProperty(builder, driverClass, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_DRIVER_PROPS.setProperty(builder, driverProperties, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_JDBC_URL.setProperty(builder, jdbcUrl, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_USERNAME.setProperty(builder, username, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_PASSWORD.setProperty(builder, password, VISIBILITY.getVisibility());

        DataConnection dc = createDataConnectionFromVertex(builder.save(authorizations));
        graph.flush();

        return dc;
    }

    @Override
    public void updateDataConnection(String id, String name, String description, String driverClass, String driverProperties, String jdbcUrl, String username, String password) {
        name = StringUtils.trimIfNull(name);
        description = StringUtils.trimIfNull(description);

        Vertex vertex = graph.getVertex(id, FetchHints.ALL, authorizations);
        ExistingElementMutation<Vertex> mutation = vertex.prepareMutation();

        DataConnectionSchema.DC_NAME.setProperty(mutation, name, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_DESCRIPTION.setProperty(mutation, description, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_DRIVER_CLASS.setProperty(mutation, driverClass, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_DRIVER_PROPS.setProperty(mutation, driverProperties, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_JDBC_URL.setProperty(mutation, jdbcUrl, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_USERNAME.setProperty(mutation, username, VISIBILITY.getVisibility());
        DataConnectionSchema.DC_PASSWORD.setProperty(mutation, password, VISIBILITY.getVisibility());
        vertex = mutation.save(authorizations);

        dcVertexCache.put(id, vertex);

        graph.flush();
    }

    @Override
    public void deleteDataConnection(String id) {
        Iterable<DataSource> dataSources = getDataSources(id);
        if(dataSources != null) {
            dataSources.forEach(dataSource -> deleteDataSource(dataSource.getId()));
        }
        graph.deleteVertex(id, authorizations);

        graph.flush();
    }

    @Override
    public Iterable<DataSource> getDataSources(String dataConnectionId) {
        Vertex dataConnectionVertex = findByIdDataConnectionVertex(dataConnectionId, FetchHints.ALL);
        return stream(dataConnectionVertex.getVertices(Direction.OUT, DataConnectionSchema.DATA_CONNECTION_TO_DATA_SOURCE_EDGE_NAME, authorizations))
                .map((Vertex dsVertex) -> createDataSourceFromVertex(dsVertex))
                .collect(Collectors.toSet());
    }

    @Override
    public DataSource findDsById(String dataSourceId) {
        return createDataSourceFromVertex(findByIdDataSourceVertex(dataSourceId, graph.getDefaultFetchHints()));
    }

    @Override
    public DataSource createDataSource(ClientApiDataSource data) {
        Vertex dataConnectionVertex = findByIdDataConnectionVertex(data.getDcId(), FetchHints.ALL);
        if(dataConnectionVertex == null)
            throw new BcException("Cannot find DataConnection with id="+data.getDcId());

        String id = GRAPH_DS_ID_PREFIX + graph.getIdGenerator().nextId();
        VertexBuilder builder = graph.prepareVertex(id, VISIBILITY.getVisibility(), DataConnectionSchema.DATA_SOURCE_CONCEPT_NAME);
        DataConnectionSchema.DS_NAME.setProperty(builder, data.getName().trim(), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_DESCRIPTION.setProperty(builder, data.getDescription().trim(), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_MAX_RECORDS.setProperty(builder, data.getMaxRecords(), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_SQL.setProperty(builder, data.getSqlSelect().trim(), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_ENTITY_MAPPING.setProperty(builder, ClientApiConverter.clientApiToString(data.getEntityMappings()), VISIBILITY.getVisibility());
        if(data.getRelMappings() != null)
            DataConnectionSchema.DS_RELATIONSHIP_MAPPING.setProperty(builder, ClientApiConverter.clientApiToString(data.getRelMappings()), VISIBILITY.getVisibility());

        DataConnectionSchema.DS_IMPORT_CONFIG.setProperty(builder, ClientApiConverter.clientApiToString(data.getImportConfig()), VISIBILITY.getVisibility());

        Vertex dataSourceVertex = builder.save(authorizations);

        EdgeBuilder edgeBuilder = graph.prepareEdge(
                dataConnectionVertex,
                dataSourceVertex,
                DataConnectionSchema.DATA_CONNECTION_TO_DATA_SOURCE_EDGE_NAME,
                VISIBILITY.getVisibility()
        );
        edgeBuilder.save(authorizations);

        graph.flush();

        DataSource ds = createDataSourceFromVertex(dataSourceVertex);
        return ds;
    }

    @Override
    public void updateDataSource(ClientApiDataSource data) {
        Vertex vertex = graph.getVertex(data.getDsId(), FetchHints.ALL, authorizations);
        ExistingElementMutation<Vertex> mutation = vertex.prepareMutation();

        DataConnectionSchema.DS_NAME.setProperty(mutation, data.getName().trim(), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_DESCRIPTION.setProperty(mutation, data.getDescription().trim(), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_MAX_RECORDS.setProperty(mutation, data.getMaxRecords(), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_SQL.setProperty(mutation, data.getSqlSelect().trim(), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_ENTITY_MAPPING.setProperty(mutation, ClientApiConverter.clientApiToString(data.getEntityMappings()), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_RELATIONSHIP_MAPPING.setProperty(mutation, ClientApiConverter.clientApiToString(data.getRelMappings()), VISIBILITY.getVisibility());
        DataConnectionSchema.DS_IMPORT_CONFIG.setProperty(mutation, ClientApiConverter.clientApiToString(data.getImportConfig()), VISIBILITY.getVisibility());

        vertex = mutation.save(authorizations);

        dsVertexCache.put(data.getDsId(), vertex);

        graph.flush();
    }

    @Override
    public void deleteDataSource(String dataSourceId) {
        graph.deleteVertex(dataSourceId, authorizations);
        graph.flush();
    }

    @Override
    public DataSourceManager getDataSourceManager() {
        return dataSourceManager;
    }

    @Override
    public ClientApiDataConnections toClientApi(Iterable<DataConnection> dataConnections) {
        ClientApiDataConnections clientApi = new ClientApiDataConnections();
        for(DataConnection dataConnection : dataConnections)
            clientApi.getDataConnections().add(toClientApi(dataConnection));

        return clientApi;
    }

    @Override
    public ClientApiDataConnection toClientApi(DataConnection dataConnection) {
        ClientApiDataConnection clientApi = new ClientApiDataConnection();
        clientApi.setId(dataConnection.getId());
        clientApi.setName(dataConnection.getName());
        clientApi.setDescription(dataConnection.getDescription());
        clientApi.setDriverClass(dataConnection.getDriverClass());
        clientApi.setDriverProperties(dataConnection.getDriverProperties());
        clientApi.setJdbcUrl(dataConnection.getJdbcUrl());
        clientApi.setUsername(dataConnection.getUsername());
        clientApi.setPassword(dataConnection.getPassword());

        Iterable<DataSource> dataSources = getDataSources(dataConnection.getId());
        for(DataSource ds : dataSources)
            clientApi.getDataSources().add(toClientApi(dataConnection.getId(), ds));

        return clientApi;
    }

    @Override
    public ClientApiDataSource toClientApi(String dataConnectionId, DataSource dataSource) {
        ClientApiDataSource clientApi = new ClientApiDataSource();
        clientApi.setDcId(dataConnectionId);
        clientApi.setDsId(dataSource.getId());
        clientApi.setName(dataSource.getName());
        clientApi.setDescription(dataSource.getDescription());
        clientApi.setMaxRecords(dataSource.getMaxRecords());
        clientApi.setSqlSelect(dataSource.getSqlSelect());

        if(dataSource.getLastImportDate() != null) {
            String dateFormat =
                    configuration.get(Configuration.WEB_CONFIGURATION_PREFIX + "formats.date.dateDisplay", "dd/MM/yyyy");
            String timeFormat =
                    configuration.get(Configuration.WEB_CONFIGURATION_PREFIX + "formats.date.timeDisplay", "HH:mm");

            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat + " " + timeFormat);
            clientApi.setLastImportDate(sdf.format(dataSource.getLastImportDate()));
        }

        clientApi.setImportRunning(dataSource.isRunning());
        clientApi.setEntityMappings(dataSource.getEntityMapping());
        clientApi.setRelMappings(dataSource.getRelMapping());
        clientApi.setImportConfig(dataSource.getImportConfig());
        return clientApi;
    }

    private GeDataConnection createDataConnectionFromVertex(Vertex vertex) {
        if (vertex == null) {
            return null;
        }

        return new GeDataConnection(vertex);
    }

    private GeDataSource createDataSourceFromVertex(Vertex vertex) {
        if (vertex == null) {
            return null;
        }

        return new GeDataSource(vertex);
    }

    @Traced
    public Vertex findByIdDataConnectionVertex(String id, FetchHints fetchHints) {
        Vertex vertex = dcVertexCache.getIfPresent(id);
        if (vertex != null) {
            return vertex;
        }
        vertex = graph.getVertex(id, fetchHints, authorizations);
        if (vertex != null) {
            dcVertexCache.put(id, vertex);
        }
        return vertex;
    }

    @Traced
    public Vertex findByIdDataSourceVertex(String id, FetchHints fetchHints) {
        Vertex vertex = dsVertexCache.getIfPresent(id);
        if (vertex != null) {
            return vertex;
        }
        vertex = graph.getVertex(id, fetchHints, authorizations);
        if (vertex != null) {
            dsVertexCache.put(id, vertex);
        }
        return vertex;
    }

    @Override
    public void setImportRunning(String dataSourceId, boolean running) {
        Vertex vertex = graph.getVertex(dataSourceId, FetchHints.PROPERTIES_AND_METADATA, authorizations);
        ExistingElementMutation<Vertex> mutation = vertex.prepareMutation();

        DataConnectionSchema.DS_IMPORT_RUNNING.setProperty(mutation, Boolean.valueOf(running), VISIBILITY.getVisibility());
        vertex = mutation.save(authorizations);

        dsVertexCache.put(dataSourceId, vertex);
        graph.flush();
    }

    @Override
    public void setLastImportDate(String dataSourceId, ZonedDateTime lastRun) {
        Vertex vertex = graph.getVertex(dataSourceId, FetchHints.PROPERTIES_AND_METADATA, authorizations);
        ExistingElementMutation<Vertex> mutation = vertex.prepareMutation();

        DataConnectionSchema.DS_LAST_IMPORT_DATE.setProperty(mutation, lastRun, VISIBILITY.getVisibility());
        vertex = mutation.save(authorizations);

        dsVertexCache.put(dataSourceId, vertex);
        graph.flush();
    }
}
