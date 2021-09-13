package com.mware.ge.cypher;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.config.FileConfigurationLoader;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.security.AuditService;
import com.mware.core.security.AuthTokenService;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.collection.RawIterator;
import com.mware.ge.cypher.builtin.proc.*;
import com.mware.ge.cypher.builtin.proc.datetime.DateFunctions;
import com.mware.ge.cypher.builtin.proc.dbms.BuiltInDbmsProcedures;
import com.mware.ge.cypher.builtin.proc.jdbc.JdbcProcedures;
import com.mware.ge.cypher.builtin.proc.spatial.SpecialBuiltInProcedures;
import com.mware.ge.cypher.connection.NetworkConnectionTracker;
import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.cypher.internal.CacheTracer;
import com.mware.ge.cypher.internal.CypherConfiguration;
import com.mware.ge.cypher.internal.StringCacheMonitor;
import com.mware.ge.cypher.internal.compiler.CypherPlannerConfiguration;
import com.mware.ge.cypher.internal.javacompat.MonitoringCacheTracer;
import com.mware.ge.cypher.internal.tracing.CompilationTracer;
import com.mware.ge.cypher.internal.tracing.TimingCompilationTracer;
import com.mware.ge.cypher.procedure.exec.ProcedureConfig;
import com.mware.ge.cypher.procedure.exec.Procedures;
import com.mware.ge.cypher.procedure.impl.CallableProcedure;
import com.mware.ge.cypher.procedure.impl.Context;
import com.mware.ge.cypher.procedure.impl.QualifiedName;
import com.mware.ge.cypher.security.SecurityContext;
import com.mware.ge.cypher.values.utils.ValueUtils;
import com.mware.ge.dependencies.Dependencies;
import com.mware.ge.dependencies.DependencyResolver;
import com.mware.ge.io.CloseableResourceManager;
import com.mware.ge.io.CpuClock;
import com.mware.ge.io.HeapAllocation;
import com.mware.ge.time.SystemNanoClock;
import com.mware.ge.type.GeoPoint;
import com.mware.ge.type.GeoShape;
import com.mware.ge.values.virtual.MapValue;

import java.nio.file.Paths;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.mware.ge.cypher.builtin.proc.datetime.TemporalFunction.registerTemporalFunctions;
import static com.mware.ge.cypher.builtin.proc.spatial.GeoFunction.registerGeoFunctions;
import static com.mware.ge.cypher.procedure.impl.Context.*;
import static com.mware.ge.cypher.procedure.impl.Neo4jTypes.*;

@Singleton
public class GeCypherExecutionEngine {
    private final BcLogger LOGGER = BcLoggerFactory.getLogger(GeCypherExecutionEngine.class);
    
    private final VisibilityTranslator visibilityTranslator = new VisibilityTranslator();
    private final CypherConfiguration cypherConfig;
    private final InternalCypherExecutionEngine executionEngine;
    private final Procedures procedures;
    private final Monitors monitors = new Monitors();
    private final Dependencies dependencyResolver = new Dependencies();
    private final GraphWithSearchIndex graph;
    private SchemaRepository schemaRepository;
    private final LifeSupportService lifeSupportService;
    private final UserRepository userRepository;
    private TermMentionRepository termMentionRepository;
    private AuthorizationRepository authorizationRepository;
    private WorkQueueRepository workQueueRepository;
    private AuditService auditService;
    private AuthTokenService authTokenService;
    private final WorkspaceRepository workspaceRepository;
    private final GraphRepository graphRepository;
    private NetworkConnectionTracker connectionTracker;

    @Inject
    public GeCypherExecutionEngine(
            Graph graph,
            SchemaRepository schemaRepository,
            LifeSupportService lifeSupportService,
            UserRepository userRepository,
            AuthorizationRepository authorizationRepository,
            WorkQueueRepository workQueueRepository,
            AuditService auditService,
            AuthTokenService authTokenService,
            TermMentionRepository termMentionRepository,
            WorkspaceRepository workspaceRepository,
            GraphRepository graphRepository,
            NetworkConnectionTracker connectionTracker
    ) {
        this.graph = (GraphWithSearchIndex) graph;
        this.schemaRepository = schemaRepository;
        this.lifeSupportService = lifeSupportService;
        this.userRepository = userRepository;
        this.authorizationRepository = authorizationRepository;
        this.workQueueRepository = workQueueRepository;
        this.auditService = auditService;
        this.authTokenService = authTokenService;
        this.termMentionRepository = termMentionRepository;
        this.workspaceRepository = workspaceRepository;
        this.graphRepository = graphRepository;
        this.connectionTracker = connectionTracker;

        Map<String, String> initialConfig = new HashMap<>();
        initialConfig.put("dbms.security.procedures.unrestricted", "algo.*");

        procedures = setupProcedures();
        lifeSupportService.add(procedures);
        cypherConfig = CypherConfiguration.fromConfig();
        CacheTracer cacheTracer = new MonitoringCacheTracer(monitors.newMonitor(StringCacheMonitor.class));
        CompilationTracer tracer = new TimingCompilationTracer(monitors.newMonitor(TimingCompilationTracer.EventListener.class));
        CypherPlannerConfiguration plannerConfig = cypherConfig.toCypherPlannerConfiguration();

        AtomicReference<CpuClock> cpuClockRef = new AtomicReference<>(CpuClock.CPU_CLOCK);
        AtomicReference<HeapAllocation> heapAllocationRef = new AtomicReference<>(HeapAllocation.HEAP_ALLOCATION);
        QueryRegistrationOperations queryRegistrationOperations = new StackingQueryRegistrationOperations(SystemNanoClock.INSTANCE, cpuClockRef, heapAllocationRef);

        dependencyResolver.satisfyDependencies(queryRegistrationOperations);
        dependencyResolver.satisfyDependencies(new StatementOperationParts(queryRegistrationOperations));
        dependencyResolver.satisfyDependencies(visibilityTranslator);
        dependencyResolver.satisfyDependency(connectionTracker);

        if (termMentionRepository != null) {
            dependencyResolver.satisfyDependencies(termMentionRepository);
        }
        dependencyResolver.satisfyDependencies(graphRepository);

        executionEngine = new InternalCypherExecutionEngine(this, tracer, cacheTracer, cypherConfig, Clock.systemUTC(), plannerConfig);
    }

    private Procedures setupProcedures() {
        ProcedureConfig procedureConfig = new ProcedureConfig();
        String bcDir = System.getenv(FileConfigurationLoader.ENV_BC_DIR);
        Procedures procedures =
                new Procedures(new SpecialBuiltInProcedures(), Paths.get(bcDir + "/lib/cypher").toFile(),
                        procedureConfig);
        dependencyResolver.satisfyDependencies(procedures);

        procedures.registerType(Vertex.class, NTNode);
        procedures.registerType(Edge.class, NTRelationship);
        procedures.registerType(Element.class, NTAny);
        procedures.registerType(GeoPoint.class, NTPoint);
        procedures.registerType(GeoShape.class, NTGeometry);

        BcLogger proceduresLog = BcLoggerFactory.getLogger(Procedures.class);
        procedures.registerComponent(BcLogger.class, ctx -> proceduresLog, true);
        procedures.registerComponent(DependencyResolver.class, ctx -> ctx.get(DEPENDENCY_RESOLVER), false);
        procedures.registerComponent(GeCypherQueryContext.class, ctx -> ctx.get(CYPHER_QUERY_CONTEXT), false);
        procedures.registerComponent(SecurityContext.class, ctx -> ctx.get(SECURITY_CONTEXT), true);

        procedures.register(new UserRepositoryProcedures.GetUserProcedure(userRepository));
        procedures.register(new ClusterProcedures.ClusterRoleProcedure());
        procedures.registerProcedure(BuiltInProcedures.class);
        procedures.registerProcedure(BuiltInDbmsProcedures.class);
        procedures.registerBuiltInFunctions(BuiltInFunctions.class);
        procedures.registerBuiltInFunctions(DateFunctions.class);
        procedures.registerProcedure(JdbcProcedures.class);
        procedures.registerProcedure(DataWorkerProcedures.class);
        procedures.registerProcedure(SchemaProcedures.class);
        registerTemporalFunctions(procedures, procedureConfig);
        registerGeoFunctions(procedures, procedureConfig);
        return procedures;
    }

    public GeCypherQueryContext newGeQueryContext(Authorizations authorizations, String workspaceId, String query, MapValue parameters) {
        return new GeCypherQueryContext(query, parameters, graph, authorizations, schemaRepository, procedures, dependencyResolver, workQueueRepository, workspaceId);
    }

    public Result executeQuery(String query, Authorizations authorizations) {
        return executeQuery(query, Collections.emptyMap(), authorizations, SchemaRepository.PUBLIC);
    }

    public Result executeQuery(String query, Authorizations authorizations, String workspaceId) {
        return executeQuery(query, Collections.emptyMap(), authorizations, workspaceId);
    }

    public Result executeQuery(String query, MapValue parameters, Authorizations authorizations) {
        return executeQuery(query, parameters, authorizations, SchemaRepository.PUBLIC);
    }

    public Result executeQuery(String query, MapValue parameters, Authorizations authorizations, String workspaceId) {
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("%s ### {PARAMS: %s} {AUTHS: %s} {WS: %s} ###", query, parameters.toString(), authorizations.toString(), workspaceId);

        return executionEngine.execute(query, parameters, newGeQueryContext(authorizations, workspaceId, query, parameters), false);
    }

    public Result executeQuery(String query, Map<String, Object> parameters, Authorizations authorizations, String workspaceId) {
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("%s ### {PARAMS: %s} {AUTHS: %s} {WS: %s} ###", query, parameters.toString(), authorizations.toString(), workspaceId);

        return executionEngine.execute(query, ValueUtils.asParameterMapValue(parameters), newGeQueryContext(authorizations, workspaceId, query, ValueUtils.asParameterMapValue(parameters)), false);
    }

    public Result profileQuery(String query, Authorizations authorizations, String workspaceId) {
        return profileQuery(query, Collections.emptyMap(), authorizations, workspaceId);
    }

    public Result profileQuery(String query, Map<String, Object> parameters, Authorizations authorizations, String workspaceId) {
        return executionEngine.execute(query, ValueUtils.asParameterMapValue(parameters), newGeQueryContext(authorizations, workspaceId, query, ValueUtils.asParameterMapValue(parameters)), true);
    }

    public void registerProcedure(CallableProcedure procedure) throws ProcedureException {
        procedures.register(procedure);
    }

    public RawIterator<Object[], ProcedureException> callProcedure(Context ctx, int id, Object[] arguments) throws ProcedureException {
        return procedures.callProcedure(ctx, id, arguments, new CloseableResourceManager());
    }

    public RawIterator<Object[], ProcedureException> callProcedure(Context ctx, QualifiedName name, Object[] arguments) throws ProcedureException {
        return procedures.callProcedure(ctx, name, arguments, new CloseableResourceManager());
    }

    public DependencyResolver getDependencyResolver() {
        return dependencyResolver;
    }

    public GraphWithSearchIndex getGraph() {
        return graph;
    }

    public SchemaRepository getSchemaRepository() {
        return schemaRepository;
    }

    public Procedures getProcedures() {
        return procedures;
    }

    public Monitors getMonitors() {
        return monitors;
    }

    public UserRepository getUserRepository() {
        return userRepository;
    }

    public AuthorizationRepository getAuthorizationRepository() {
        return authorizationRepository;
    }

    public AuditService getAuditService() {
        return auditService;
    }

    public AuthTokenService getAuthTokenService() {
        return authTokenService;
    }

    public WorkspaceRepository getWorkspaceRepository() {
        return workspaceRepository;
    }
}
