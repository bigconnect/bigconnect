package com.mware.ge.cypher.builtin.proc;

import com.mware.core.user.SystemUser;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.cypher.procedure.Context;
import com.mware.ge.cypher.procedure.Description;
import com.mware.ge.cypher.procedure.Name;
import com.mware.ge.cypher.procedure.Procedure;

public class SchemaProcedures {
    @Context
    public GeCypherQueryContext bcApi;

    @Procedure(name = "schema.deleteProperty")
    @Description("schema.deleteProperty(namespace, propertyName).")
    public void deleteProperty(
            @Name(value = "namespace") String namespace,
            @Name(value = "propertyName") String propertyName
    ) {
        bcApi.getSchemaRepository().deleteProperty(
                propertyName,
                new SystemUser(),
                namespace
        );
        bcApi.getSchemaRepository().clearCache(namespace);
    }

    @Procedure(name = "schema.deleteConcept")
    @Description("schema.deleteConcept(namespace, conceptName).")
    public void deleteConcept(
            @Name(value = "namespace") String namespace,
            @Name(value = "conceptName") String conceptName
    ) {
        bcApi.getSchemaRepository().deleteConcept(
                conceptName,
                new SystemUser(),
                namespace
        );
        bcApi.getSchemaRepository().clearCache(namespace);
    }

    @Procedure(name = "schema.deleteRelationship")
    @Description("schema.deleteRelationship(namespace, relName).")
    public void deleteRelationship(
            @Name(value = "namespace") String namespace,
            @Name(value = "relName") String relName
    ) {
        bcApi.getSchemaRepository().deleteRelationship(
                relName,
                new SystemUser(),
                namespace
        );
        bcApi.getSchemaRepository().clearCache(namespace);
    }
}
