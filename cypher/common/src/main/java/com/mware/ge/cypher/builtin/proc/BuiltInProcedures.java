/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.ge.cypher.builtin.proc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mware.core.model.clientapi.dto.ClientApiSchema;
import com.mware.core.model.clientapi.util.ObjectMapperFactory;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.Relationship;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.util.StreamUtil;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.cypher.procedure.Context;
import com.mware.ge.cypher.procedure.Description;
import com.mware.ge.cypher.procedure.Name;
import com.mware.ge.cypher.procedure.Procedure;
import com.mware.ge.cypher.procedure.exec.Procedures;
import com.mware.ge.cypher.procedure.impl.ProcedureSignature;
import com.mware.ge.cypher.procedure.impl.UserFunctionSignature;
import com.mware.ge.dependencies.DependencyResolver;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static com.mware.ge.cypher.procedure.Mode.DBMS;
import static com.mware.ge.cypher.procedure.Mode.READ;

@SuppressWarnings({"unused", "WeakerAccess"})
public class BuiltInProcedures {
    @Context
    public DependencyResolver resolver;

    @Context
    public GeCypherQueryContext bcApi;

    @Description("List all concept types in the system.")
    @Procedure(name = "db.labels", mode = READ)
    public Stream<LabelResult> listLabels() {
        Collection<Concept> concepts = bcApi.getSchemaRepository().getOntology(SchemaRepository.PUBLIC).getConcepts();
        return StreamUtil.stream(concepts)
                .filter(Concept::getUserVisible)
                .map(c -> new LabelResult(c.getName()));
    }

    @Description("List all relationship types in the database.")
    @Procedure(name = "db.relationshipTypes", mode = READ)
    public Stream<RelationshipTypeResult> listRelationshipTypes() {
        Collection<Relationship> rels = bcApi.getSchemaRepository().getOntology(SchemaRepository.PUBLIC).getRelationships();
        return StreamUtil.stream(rels)
                .filter(Relationship::getUserVisible)
                .map(r -> new RelationshipTypeResult(r.getName()));
    }

    @Description("List all properties in the database.")
    @Procedure(name = "db.propertyKeys", mode = READ)
    public Stream<PropertyKeyResult> listPropertyKeys() {
        Collection<SchemaProperty> props = bcApi.getSchemaRepository().getOntology(SchemaRepository.PUBLIC).getProperties();
        return StreamUtil.stream(props)
                .filter(SchemaProperty::getUserVisible)
                .map(p -> new PropertyKeyResult(p.getName()));
    }

    @Description("List all concept types in the system.")
    @Procedure(name = "db.schemaConcepts", mode = READ)
    public Stream<OntologyConceptResult> listSchemaConcepts() {
        Collection<Concept> concepts = bcApi.getSchemaRepository().getOntology(SchemaRepository.PUBLIC).getConcepts();
        return StreamUtil.stream(concepts)
                .filter(Concept::getUserVisible)
                .map(c -> new OntologyConceptResult(c.getName(), c.getDisplayName()));
    }

    @Description("List all relationship types in the database.")
    @Procedure(name = "db.schemaRelationships", mode = READ)
    public Stream<OntologyRelationshipResult> listSchemaRelationships() {
        Collection<Relationship> rels = bcApi.getSchemaRepository().getOntology(SchemaRepository.PUBLIC).getRelationships();
        return StreamUtil.stream(rels)
                .filter(Relationship::getUserVisible)
                .map(r -> new OntologyRelationshipResult(r.getName(), r.getDisplayName()));
    }

    @Description("List all properties for a concept")
    @Procedure(name = "db.schemaProperties", mode = READ)
    public Stream<OntologyPropertyResult> listSchemaProperties(
            @Name(value = "conceptName") String conceptName
    ) {
        final Concept concept = bcApi.getSchemaRepository().getConceptByName(conceptName, SchemaRepository.PUBLIC);
        Set<Concept> allConcepts = bcApi.getSchemaRepository().getConceptAndAncestors(concept, SchemaRepository.PUBLIC);
        Set<SchemaProperty> allProps = new HashSet<>();
        allConcepts.forEach(c -> allProps.addAll(c.getProperties()));

        return StreamUtil.stream(allProps)
                .filter(SchemaProperty::getUserVisible)
                .map(p -> new OntologyPropertyResult(p.getName(), p.getDisplayName(), p.getDataType().getText()));
    }

    @Description("Get schema JSON")
    @Procedure(name = "db.schemaJson", mode = READ)
    public Stream<OntologyResult> listSchemaJson() throws JsonProcessingException {
        ClientApiSchema schema = bcApi.getSchemaRepository().getClientApiObject();
        String result = ObjectMapperFactory.getInstance().writeValueAsString(schema);
        return Stream.of(new OntologyResult(result));
    }

    @Description("List all procedures in the DBMS.")
    @Procedure(name = "dbms.procedures", mode = DBMS)
    public Stream<ProcedureResult> listProcedures() {
        return bcApi.getDependencyResolver().resolveDependency(Procedures.class).getAllProcedures().stream()
                .sorted(Comparator.comparing(a -> a.name().toString()))
                .map(ProcedureResult::new);
    }

    @Description("List all user functions in the DBMS.")
    @Procedure(name = "dbms.functions", mode = DBMS)
    public Stream<FunctionResult> listFunctions() {
        return bcApi.getDependencyResolver().resolveDependency(Procedures.class).getAllFunctions().stream()
                .sorted(Comparator.comparing(a -> a.name().toString()))
                .map(FunctionResult::new);
    }

    public static class LabelResult {
        public final String label;

        private LabelResult(String label) {
            this.label = label;
        }
    }

    public static class RelationshipTypeResult {
        public final String relationshipType;

        private RelationshipTypeResult(String relationshipType) {
            this.relationshipType = relationshipType;
        }
    }

    public static class PropertyKeyResult {
        public final String propertyKey;

        private PropertyKeyResult(String propertyKey) {
            this.propertyKey = propertyKey;
        }
    }

    public static class OntologyConceptResult {
        public final String name;
        public final String displayName;

        private OntologyConceptResult(String name, String displayName) {
            this.name = name;
            this.displayName = displayName;
        }
    }

    public static class OntologyRelationshipResult {
        public final String name;
        public final String displayName;

        private OntologyRelationshipResult(String name, String displayName) {
            this.name = name;
            this.displayName = displayName;
        }
    }

    public static class OntologyPropertyResult {
        public final String name;
        public final String displayName;
        public final String type;

        private OntologyPropertyResult(String name, String displayName, String type) {
            this.name = name;
            this.displayName = displayName;
            this.type = type;
        }
    }

    public static class OntologyResult {
        public final String json;

        public OntologyResult(String json) {
            this.json = json;
        }
    }

    public static class FunctionResult {
        public final String name;
        public final String signature;
        public final String description;

        private FunctionResult(UserFunctionSignature signature) {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse("");
        }
    }

    public static class ProcedureResult {
        public final String name;
        public final String signature;
        public final String description;
        public final String mode;

        private ProcedureResult(ProcedureSignature signature) {
            this.name = signature.name().toString();
            this.signature = signature.toString();
            this.description = signature.description().orElse("");
            this.mode = signature.mode().toString();
        }
    }
}
