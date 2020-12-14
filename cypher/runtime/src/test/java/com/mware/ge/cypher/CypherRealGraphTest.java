package com.mware.ge.cypher;

import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.cmdline.CommandLineTool;

import java.util.Map;

public class CypherRealGraphTest extends CommandLineTool  {
    GeCypherExecutionEngine executionEngine;

    @Override
    protected int run() throws Exception {
        executionEngine = InjectHelper.getInstance(GeCypherExecutionEngine.class);

        executeQuery("CREATE (f:fbPost) SET f.last_modified = timestamp()");
        executeQuery("MATCH (f:fbPost) RETURN f");

        return 0;
    }

    private void executeQuery(String q) {
        long startTime = System.currentTimeMillis();
        Result r = executionEngine.executeQuery(q, getAuthorizations(), "public-ontology");
        consumeResult(r);
        System.out.println("Took: "+(System.currentTimeMillis() - startTime)/1000);
    }

    private void consumeResult(Result r) {
        if (r == null)
            return;

        while (r.hasNext()) {
            Map<String, Object> next = r.next();
            System.out.println(next);
        }
    }

    public static void main(String[] args) throws Exception {
        CommandLineTool.main(new CypherRealGraphTest(), args);
    }
}
