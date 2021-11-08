package com.mware.core.trace;

import com.google.inject.Inject;
import com.mware.core.config.Configuration;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.lifecycle.LifecycleAdapter;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.user.GraphAuthorizationRepository;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.*;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.util.Preconditions;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Values;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mware.core.model.schema.SchemaConstants.CONCEPT_TYPE_THING;

public class ElementTracer {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(ElementTracer.class);
    private static final String TRACER_VERTEX_ID = "trace_vertex";
    private static final String TRACER_TABLE = "trace_table";
    private static final Authorizations AUTHORIZATIONS = new Authorizations(AuthorizationRepository.ADMIN_ROLE);
    private static final Visibility VISIBILITY = new Visibility(AuthorizationRepository.ADMIN_ROLE);

    private final Graph graph;
    private final boolean enabled;

    @Inject
    public ElementTracer(Graph graph, Configuration configuration, GraphAuthorizationRepository graphAuthorizationRepository) {
        this.graph = graph;
        this.enabled = configuration.getBoolean("elementTracer.enabled", false);
        graphAuthorizationRepository.addAuthorizationToGraph(AuthorizationRepository.ADMIN_ROLE);
        ensureTraceVertexExists();
    }

    private void ensureTraceVertexExists() {
        if (!this.graph.doesVertexExist(TRACER_VERTEX_ID, AUTHORIZATIONS)) {
            this.graph
                    .prepareVertex(TRACER_VERTEX_ID, VISIBILITY, CONCEPT_TYPE_THING)
                    .save(AUTHORIZATIONS);
            this.graph.flush();

            graph.defineProperty("eid").dataType(TextValue.class).textIndexHint(TextIndexHint.EXACT_MATCH).define();
        }
    }

    public void trace(String elementId, String action, String description) {
        if (!enabled) {
            LOGGER.debug("%s, %s, %s", elementId, action, description);
            return;
        }

        Preconditions.checkNotNull(elementId);
        String rowId = graph.getIdGenerator().nextId();
        this.graph.getVertex(TRACER_VERTEX_ID, AUTHORIZATIONS).prepareMutation()
                .addExtendedData(TRACER_TABLE, rowId, "eid", Values.stringValue(elementId), VISIBILITY)
                .addExtendedData(TRACER_TABLE, rowId, "a", Values.stringValue(action), VISIBILITY)
                .addExtendedData(TRACER_TABLE, rowId, "d", Values.stringValue(description), VISIBILITY)
                .addExtendedData(TRACER_TABLE, rowId, "t", DateTimeValue.now(Clock.systemUTC()), VISIBILITY)
                .save(AUTHORIZATIONS);
    }

    public List<ElementTraceInfo> getTraces(String elementId) {
        try (QueryResultsIterable<ExtendedDataRow> rows = this.graph.getVertex(TRACER_VERTEX_ID, AUTHORIZATIONS).getExtendedData(TRACER_TABLE)
                .query(AUTHORIZATIONS)
                .has("eid", Values.stringValue(elementId))
                .extendedDataRows()) {

            List<ElementTraceInfo> traces = new ArrayList<>();
            for (ExtendedDataRow row : rows) {
                TextValue action = (TextValue) row.getPropertyValue("a");
                TextValue desc = (TextValue) row.getPropertyValue("d");
                DateTimeValue ts = (DateTimeValue) row.getPropertyValue("t");
                traces.add(new ElementTraceInfo(action.stringValue(), desc.stringValue(), ts.asObjectCopy().toInstant().toEpochMilli()));
            }
            return traces;
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return Collections.emptyList();
    }
}
