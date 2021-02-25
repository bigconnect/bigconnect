package com.mware.ge.cypher;

import com.mware.core.GraphTestBase;
import com.mware.core.lifecycle.LifeSupportService;
import com.mware.core.security.AuthTokenService;
import com.mware.core.security.DirectVisibilityTranslator;
import com.mware.ge.Authorizations;
import com.mware.ge.base.TestGraphFactory;
import com.mware.ge.cypher.connection.NetworkConnectionTracker;
import com.mware.ge.inmemory.InMemoryGraphFactory;
import org.junit.Test;

import java.util.Map;

public class CypherQueriesTest extends GraphTestBase {
    @Test
    public void test1() {
        GeCypherExecutionEngine ee = new GeCypherExecutionEngine(
                getGraph(), getSchemaRepository(), new LifeSupportService(), getUserRepository(), null, null, null, null, null, null, null, null, NetworkConnectionTracker.NO_OP);
        Authorizations authorizations = getGraphAuthorizations();

        ee.executeQuery("CREATE (n1 {prop: 1}), (n2 {prop: 3}), (n3 {prop: -5})", authorizations);
        ee.executeQuery("MATCH (n) RETURN id(n)", authorizations);
        ee.executeQuery("MATCH ()-[r]->() RETURN id(r)", authorizations);
        ee.executeQuery("MATCH (n) UNWIND labels(n) AS label RETURN DISTINCT label", authorizations);
        ee.executeQuery("MATCH (n) UNWIND keys(n) AS key WITH properties(n) AS properties, key, n RETURN id(n) AS nodeId, key, properties[key] AS value", authorizations);
        ee.executeQuery("MATCH ()-[r]->() UNWIND keys(r) AS key WITH properties(r) AS properties, key, r RETURN id(r) AS relId, key, properties[key] AS value", authorizations);
        Result r = ee.executeQuery("CYPHER planner=cost runtime=interpreted MATCH (n) RETURN n.prop AS prop ORDER BY n.prop", authorizations);

        while(r.hasNext()) {
            System.out.println(r.next());
        }
    }

    @Test
    public void test2() {
        GeCypherExecutionEngine ee = new GeCypherExecutionEngine(
                getGraph(), getSchemaRepository(), new LifeSupportService(),
                getUserRepository(), getAuthorizationRepository(), null, getAuditService(), new AuthTokenService(getConfiguration(), getUserRepository()),
                new DirectVisibilityTranslator(), null, getWorkspaceRepository(), null,
                NetworkConnectionTracker.NO_OP
        );
        Authorizations authorizations = getGraphAuthorizations();

        ee.executeQuery("CREATE (a:A) CREATE (a)-[:T]->(:B), (a)-[:T]->(:C)", authorizations);

        Result r = ee.executeQuery("MATCH (n)-->(b) WITH [p = (n)-->() | p] AS ps, count(b) AS c RETURN ps, c", authorizations);
        while(r.hasNext()) {
            Map<String, Object> result = r.next();
            System.out.println(result);
        }
    }

    @Override
    protected TestGraphFactory graphFactory() {
        return new InMemoryGraphFactory();
    }
}
