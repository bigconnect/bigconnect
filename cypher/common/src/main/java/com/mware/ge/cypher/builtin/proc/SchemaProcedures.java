package com.mware.ge.cypher.builtin.proc;

import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.model.schema.*;
import com.mware.core.user.SystemUser;
import com.mware.ge.TextIndexHint;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.cypher.procedure.Context;
import com.mware.ge.cypher.procedure.Description;
import com.mware.ge.cypher.procedure.Name;
import com.mware.ge.cypher.procedure.Procedure;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Map;

public class SchemaProcedures {
    @Context
    public GeCypherQueryContext bcApi;

    @Procedure(name = "schema.addRelProperty")
    @Description("Adds a new property to a BigConnect relationship")
    public void addRelProperty(
            @Name(value = "rel") String rel,
            @Name(value = "name") String name,
            @Name(value = "type") String type,
            @Name(value = "displayName", defaultValue = "") String displayName,
            @Name(value = "props", defaultValue = "{}") Map<String, Object> props,
            @Name(value = "namespace", defaultValue = SchemaRepository.PUBLIC) String namespace
    ) {
        Relationship _rel = bcApi.getSchemaRepository().getRelationshipByName(rel);
        SchemaFactory factory = new SchemaFactory(bcApi.getSchemaRepository());
        SchemaFactory.DefaultConceptProperty prop = factory.newConceptProperty()
                .concepts(factory.getOrCreateThingConcept())
                .forRelationships(_rel)
                .name(name)
                .type(PropertyType.valueOf(type))
                .displayName(StringUtils.isEmpty(displayName) ? name : displayName)
                .userVisible(true);

        if (props.containsKey("displayType"))
            prop.displayType((String) props.get("displayType"));
        if (props.containsKey("propertyGroup"))
            prop.propertyGroup((String) props.get("propertyGroup"));
        if (props.containsKey("displayFormula"))
            prop.displayFormula((String) props.get("displayFormula"));
        if (props.containsKey("validationFormula"))
            prop.validationFormula((String) props.get("validationFormula"));
        if (props.containsKey("userVisible"))
            prop.userVisible((Boolean) props.get("userVisible"));
        if (props.containsKey("searchFacet"))
            prop.searchFacet((Boolean) props.get("searchFacet"));
        if (props.containsKey("sortable"))
            prop.sortable((Boolean) props.get("sortable"));
        if (props.containsKey("searchable"))
            prop.searchable((Boolean) props.get("searchable"));
        if (props.containsKey("addable"))
            prop.addable((Boolean) props.get("addable"));
        if (props.containsKey("updatable"))
            prop.updatable((Boolean) props.get("updatable"));
        if (props.containsKey("deletable"))
            prop.deletable((Boolean) props.get("deletable"));
        if (props.containsKey("aggType"))
            prop.aggType((String) props.get("aggType"));
        if (props.containsKey("possibleValues"))
            prop.possibleValues((Map<String, String>) props.get("possibleValues"));
        if (props.containsKey("textIndexHints"))
            prop.textIndexHints(TextIndexHint.parse((String) props.get("textIndexHints")));

        prop.save();
        bcApi.getSchemaRepository().clearCache(namespace);
    }

    @Procedure(name = "schema.addConceptProperty")
    @Description("Adds a new property to a BigConnect concept")
    public void addConceptProperty(
            @Name(value = "concept") String concept,
            @Name(value = "name") String name,
            @Name(value = "type") String type,
            @Name(value = "displayName", defaultValue = "") String displayName,
            @Name(value = "props", defaultValue = "{}") Map<String, Object> props,
            @Name(value = "namespace", defaultValue = SchemaRepository.PUBLIC) String namespace
    ) {
        Concept _concept = bcApi.getSchemaRepository().getConceptByName(concept, namespace);
        SchemaFactory.DefaultConceptProperty prop = new SchemaFactory(bcApi.getSchemaRepository()).newConceptProperty()
                .concepts(_concept)
                .name(name)
                .type(PropertyType.valueOf(type))
                .displayName(StringUtils.isEmpty(displayName) ? name : displayName)
                .userVisible(true);

        if (props.containsKey("displayType"))
            prop.displayType((String) props.get("displayType"));
        if (props.containsKey("propertyGroup"))
            prop.propertyGroup((String) props.get("propertyGroup"));
        if (props.containsKey("displayFormula"))
            prop.displayFormula((String) props.get("displayFormula"));
        if (props.containsKey("validationFormula"))
            prop.validationFormula((String) props.get("validationFormula"));
        if (props.containsKey("userVisible"))
            prop.userVisible((Boolean) props.get("userVisible"));
        if (props.containsKey("searchFacet"))
            prop.searchFacet((Boolean) props.get("searchFacet"));
        if (props.containsKey("sortable"))
            prop.sortable((Boolean) props.get("sortable"));
        if (props.containsKey("searchable"))
            prop.searchable((Boolean) props.get("searchable"));
        if (props.containsKey("addable"))
            prop.addable((Boolean) props.get("addable"));
        if (props.containsKey("updatable"))
            prop.updatable((Boolean) props.get("updatable"));
        if (props.containsKey("deletable"))
            prop.deletable((Boolean) props.get("deletable"));
        if (props.containsKey("aggType"))
            prop.aggType((String) props.get("aggType"));
        if (props.containsKey("possibleValues"))
            prop.possibleValues((Map<String, String>) props.get("possibleValues"));
        if (props.containsKey("textIndexHints"))
            prop.textIndexHints(TextIndexHint.parse((String) props.get("textIndexHints")));

        prop.save();
        bcApi.getSchemaRepository().clearCache(namespace);
    }

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

    @Procedure(name = "schema.addConcept")
    @Description("Adds a new concept to BigConnect")
    public void addConcept(
            @Name(value = "conceptType") String conceptType,
            @Name(value = "parent", defaultValue = SchemaConstants.CONCEPT_TYPE_THING) String parent,
            @Name(value = "displayName", defaultValue = "") String displayName,
            @Name(value = "props", defaultValue = "{}") Map<String, Object> props,
            @Name(value = "namespace", defaultValue = SchemaRepository.PUBLIC) String namespace
    ) {
        Concept parentConcept = bcApi.getSchemaRepository().getConceptByName(parent, namespace);
        SchemaFactory.DefaultConcept newConcept = new SchemaFactory(bcApi.getSchemaRepository())
                .forNamespace(namespace)
                .newConcept()
                .conceptType(conceptType)
                .displayName(StringUtils.isEmpty(displayName) ? conceptType : displayName)
                .parent(parentConcept);

        // default is user visible
        if (props.isEmpty()) {
            props.put(SchemaProperties.USER_VISIBLE.getPropertyName(), Boolean.TRUE);
        }

        props.forEach((p, v) -> newConcept.property(p, Values.of(v)));

        newConcept.save();
        bcApi.getSchemaRepository().clearCache(namespace);
    }

    @Procedure(name = "schema.deleteConcept")
    @Description("schema.deleteConcept(namespace, conceptType).")
    public void deleteConcept(
            @Name(value = "namespace") String namespace,
            @Name(value = "conceptType") String conceptType
    ) {
        bcApi.getSchemaRepository().deleteConcept(
                conceptType,
                new SystemUser(),
                namespace
        );
        bcApi.getSchemaRepository().clearCache(namespace);
    }

    @Procedure(name = "schema.addRelationship")
    @Description("Adds a new relationship to BigConnect")
    public void addRelationship(
            @Name(value = "label") String edgeLabel,
            @Name(value = "source") String source,
            @Name(value = "target") String target,
            @Name(value = "props", defaultValue = "{}") Map<String, Object> props,
            @Name(value = "namespace", defaultValue = SchemaRepository.PUBLIC) String namespace
    ) {
        Concept[] sourceConcepts = Arrays.stream(source.split(","))
                .map(c -> bcApi.getSchemaRepository().getConceptByName(c, namespace))
                .toArray(Concept[]::new);
        Concept[] targetConcepts = Arrays.stream(target.split(","))
                .map(c -> bcApi.getSchemaRepository().getConceptByName(c, namespace))
                .toArray(Concept[]::new);

        SchemaFactory.DefaultRelationship newRel = new SchemaFactory(bcApi.getSchemaRepository())
                .forNamespace(namespace)
                .newRelationship()
                .label(edgeLabel)
                .source(sourceConcepts)
                .target(targetConcepts);

        // default is user visible
        if (props.isEmpty()) {
            props.put(SchemaProperties.USER_VISIBLE.getPropertyName(), Boolean.TRUE);
        }

        props.forEach((p, v) -> newRel.property(p, Values.of(v)));

        newRel.save();
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
