package com.mware.ge.cypher;

import com.mware.core.cache.CacheOptions;
import com.mware.core.cache.CacheService;
import com.mware.core.cache.InMemoryCacheService;
import com.mware.core.config.Configuration;
import com.mware.core.config.HashMapConfigurationLoader;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.schema.GeSchemaRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.schema.inmemory.InMemorySchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.model.user.InMemoryGraphAuthorizationRepository;
import com.mware.core.model.user.InMemoryUserRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WebQueueRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.AuthTokenService;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.core.security.VisibilityTranslator;
import com.mware.ge.*;
import com.mware.ge.cypher.connection.NetworkConnectionTracker;
import com.mware.ge.id.LongIdGenerator;
import com.mware.ge.id.UUIDIdGenerator;
import com.mware.ge.inmemory.InMemoryGraph;
import com.mware.ge.search.DefaultSearchIndex;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class TestCypherQueryContextFactory implements CypherQueryContextFactory {
    protected VisibilityTranslator visibilityTranslator = new DirectVisibilityTranslator();
    protected Configuration configuration;
    protected Graph graph;

    public TestCypherQueryContextFactory() throws Exception {
        createGraph();
    }

    @Override
    public GeCypherExecutionEngine emptyGraph() throws Exception {
        graph.drop();
        return createCypherExecutionEngine(new InMemoryGraphAuthorizationRepository());
    }

    protected final GeCypherExecutionEngine createCypherExecutionEngine(GraphAuthorizationRepository authorizationRepository) throws Exception {
        CacheService cacheService = new InMemoryCacheService();
        cacheService.put("__cypherAcceptance", "simpleSchema", Boolean.TRUE, new CacheOptions());

        GraphRepository graphRepository = new GraphRepository(
                graph,
                visibilityTranslator,
                Mockito.mock(TermMentionRepository.class),
                Mockito.mock(WorkQueueRepository.class),
                Mockito.mock(WebQueueRepository.class),
                configuration);


        SchemaRepository schemaRepository = new GeSchemaRepository(
                graph,
                graphRepository,
                visibilityTranslator,
                configuration,
                authorizationRepository,
                cacheService);

        return new GeCypherExecutionEngine(
                graph,
                schemaRepository,
                new LifeSupportService(),
                null,
                null,
                null,
                null,
                new AuthTokenService(configuration, null),
                visibilityTranslator,
                null,
                null,
                graphRepository,
                NetworkConnectionTracker.NO_OP
        );
    }

    protected void createGraph() throws Exception {
        File tmpDir = Files.createTempDirectory("bctests").toFile();
        tmpDir.deleteOnExit();

        Map<String, Object> config = new HashMap<>();
        config.put(GraphConfiguration.IDGENERATOR_PROP_PREFIX, UUIDIdGenerator.class.getName());
        config.put(GraphConfiguration.SEARCH_INDEX_PROP_PREFIX, DefaultSearchIndex.class.getName());
        config.put("search.inProcessNode", "false");
        config.put("search.shards", "1");
        config.put("search.replicas", "0");
        config.put("search.locations", "localhost");
        config.put("search.clusterName", "elasticsearch");
        config.put("search.indexName", "ge"+System.nanoTime());
        this.configuration = new HashMapConfigurationLoader(config).createConfiguration();
        this.graph = InMemoryGraph.create(config);
        graph.createAuthorizations("administrator");
    }
}
