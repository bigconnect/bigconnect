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
package com.mware.core.model.schema;

import com.mware.core.model.clientapi.dto.ClientApiObject;
import com.mware.core.model.clientapi.dto.ClientApiSchema;
import com.mware.core.model.properties.types.BcProperty;
import com.mware.core.security.BcVisibility;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.query.builder.BoolQueryBuilder;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SchemaRepository {
    String THING_CONCEPT_NAME = "thing";
    String ROOT_CONCEPT_NAME = "__root";
    String TYPE_RELATIONSHIP = "__or";
    String TYPE_CONCEPT = "__oc";
    String TYPE_PROPERTY = "__op";
    String VISIBILITY_STRING = "ontology";
    String CONFIG_INTENT_CONCEPT_PREFIX = "ontology.intent.concept.";
    String CONFIG_INTENT_RELATIONSHIP_PREFIX = "ontology.intent.relationship.";
    String CONFIG_INTENT_PROPERTY_PREFIX = "ontology.intent.property.";
    String PUBLIC = "public-ontology";
    BcVisibility VISIBILITY = new BcVisibility(VISIBILITY_STRING);

    void clearCache();
    void clearCache(String namespace);

    default Iterable<Relationship> getRelationships() {
        return getRelationships(SchemaRepository.PUBLIC);
    }
    Iterable<Relationship> getRelationships(String namespace);
    Iterable<Relationship> getRelationships(Iterable<String> ids, String namespace);

    default Iterable<SchemaProperty> getProperties() {
        return getProperties(PUBLIC);
    }
    Iterable<SchemaProperty> getProperties(String namespace);
    Iterable<SchemaProperty> getProperties(Iterable<String> ids, String namespace);


    default String getDisplayNameForLabel(String relationshipLabel) {
        return getDisplayNameForLabel(relationshipLabel, PUBLIC);
    }
    String getDisplayNameForLabel(String relationshipLabel, String namespace);

    default SchemaProperty getPropertyByName(String propertyName) {
        return getPropertyByName(propertyName, PUBLIC);
    }
    SchemaProperty getPropertyByName(String propertyName, String namespace);
    Iterable<SchemaProperty> getPropertiesByName(List<String> propertyNames, String namespace);

    default SchemaProperty getRequiredPropertyByName(String propertyName) {
        return getRequiredPropertyByName(propertyName, PUBLIC);
    }
    SchemaProperty getRequiredPropertyByName(String propertyName, String namespace);


    default Relationship getRelationshipByName(String relationshipName) {
        return getRelationshipByName(relationshipName, PUBLIC);
    }
    Relationship getRelationshipByName(String relationshipName, String namespace);
    Iterable<Relationship> getRelationshipsByName(List<String> relationshipNames, String namespace);


    default boolean hasRelationshipByName(String relationshipName) {
        return hasRelationshipByName(relationshipName, PUBLIC);
    }
    boolean hasRelationshipByName(String relationshipName, String namespace);

    default Iterable<Concept> getConceptsWithProperties() {
        return getConceptsWithProperties(PUBLIC);
    }
    Iterable<Concept> getConceptsWithProperties(String namespace);

    Concept getRootConcept(String namespace);

    default Concept getThingConcept() {
        return getThingConcept(PUBLIC);
    }
    Concept getThingConcept(String namespace);

    Relationship getOrCreateRootRelationship(Authorizations authorizations);

    default Concept getParentConcept(Concept concept) {
        return getParentConcept(concept, PUBLIC);
    }
    Concept getParentConcept(Concept concept, String namespace);
    Set<Concept> getAncestorConcepts(Concept concept, String namespace);
    Set<Concept> getConceptAndAncestors(Concept concept, String namespace);

    default List<Concept> getChildConcepts(Concept concept) {
        return getChildConcepts(concept, PUBLIC);
    }
    List<Concept> getChildConcepts(Concept concept, String namespace);


    default Concept getConceptByName(String conceptName) {
        return getConceptByName(conceptName, PUBLIC);
    }
    Concept getConceptByName(String conceptName, String namespace);
    Iterable<Concept> getConceptsByName(List<String> conceptNames, String namespace);

    boolean hasConceptByName(String conceptName, String namespace);

    default Set<Concept> getConceptAndAllChildrenByName(String conceptName) {
        return getConceptAndAllChildrenByName(conceptName, PUBLIC);
    }
    Set<Concept> getConceptAndAllChildrenByName(String conceptName, String namespace);


    default Set<Concept> getConceptAndAllChildren(Concept concept) {
        return getConceptAndAllChildren(concept, PUBLIC);
    }
    Set<Concept> getConceptAndAllChildren(Concept concept, String namespace);


    default Set<Relationship> getRelationshipAndAllChildren(Relationship relationship) {
        return getRelationshipAndAllChildren(relationship, PUBLIC);
    }
    Set<Relationship> getRelationshipAndAllChildren(Relationship relationship, String namespace);
    Set<Relationship> getRelationshipAndAllChildrenByName(String relationshipName, String namespace);
    Relationship getParentRelationship(Relationship relationship, String namespace);
    Set<Relationship> getAncestorRelationships(Relationship relationship, String namespace);
    Set<Relationship> getRelationshipAndAncestors(Relationship relationship, String namespace);

    Iterable<Concept> getConcepts(Iterable<String> ids, String namespace);


    default Concept getOrCreateConcept(Concept parent, String conceptName, String displayName) {
        return getOrCreateConcept(parent, conceptName, displayName, null, PUBLIC);
    }
    Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, User user, String namespace);
    Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, String glyphIconHref, String color, User user, String namespace);

    Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, boolean deleteChangeableProperties, boolean isCoreConcept);
    Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, boolean deleteChangeableProperties, boolean isCoreConcept, User user, String namespace);
    Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, String glyphIconHref, String color, boolean deleteChangeableProperties, boolean isCoreConcept, User user, String namespace);

    void deleteConcept(String conceptName, User user, String namespace);

    default Relationship getOrCreateRelationshipType(
            Relationship parent,
            Iterable<Concept> domainConcepts,
            Iterable<Concept> rangeConcepts,
            String relationshipName,
            boolean deleteChangeableProperties,
            boolean coreConcept
    ) {
        return getOrCreateRelationshipType(parent, domainConcepts, rangeConcepts, relationshipName, null, coreConcept, deleteChangeableProperties, null, PUBLIC);
    }

    Relationship getOrCreateRelationshipType(
            Relationship parent,
            Iterable<Concept> domainConcepts,
            Iterable<Concept> rangeConcepts,
            String relationshipName,
            boolean deleteChangeableProperties,
            boolean coreConcept,
            User user,
            String namespace
    );

    Relationship getOrCreateRelationshipType(
            Relationship parent,
            Iterable<Concept> domainConcepts,
            Iterable<Concept> rangeConcepts,
            String relationshipName,
            String displayName,
            boolean deleteChangeableProperties,
            boolean coreConcept,
            User user,
            String namespace
    );

    void deleteRelationship(String relationshipName, User user, String namespace);

    void addDomainConceptsToRelationshipType(String relationshipName, List<String> conceptNames, User user, String namespace);

    void addRangeConceptsToRelationshipType(String relationshipName, List<String> conceptNames, User user, String namespace);

    default SchemaProperty getOrCreateProperty(SchemaPropertyDefinition schemaPropertyDefinition) {
        return getOrCreateProperty(schemaPropertyDefinition, null, PUBLIC);
    }
    SchemaProperty getOrCreateProperty(SchemaPropertyDefinition schemaPropertyDefinition, User user, String namespace);

    void deleteProperty(String propertyName, User user, String namespace);


    default void resolvePropertyIds(JSONArray filterJson) throws JSONException {
        resolvePropertyIds(filterJson, PUBLIC);
    }
    void resolvePropertyIds(JSONArray filterJson, String namespace) throws JSONException;

    default ClientApiSchema getClientApiObject() {
        return getClientApiObject(PUBLIC);
    }
    ClientApiSchema getClientApiObject(String namespace);

    Schema getOntology(String namespace);


    default Concept getConceptByIntent(String intent) {
        return getConceptByIntent(intent, PUBLIC);
    }
    Concept getConceptByIntent(String intent, String namespace);


    default String getConceptNameByIntent(String intent) {
        return getConceptNameByIntent(intent, PUBLIC);
    }
    String getConceptNameByIntent(String intent, String namespace);


    default Concept getRequiredConceptByIntent(String intent) {
        return getRequiredConceptByIntent(intent, PUBLIC);
    }
    Concept getRequiredConceptByIntent(String intent, String namespace);

    default Concept getRequiredConceptByName(String conceptName) {
        return getRequiredConceptByName(conceptName, PUBLIC);
    }
    Concept getRequiredConceptByName(String conceptName, String namespace);


    default String getRequiredConceptNameByIntent(String intent) {
        return getRequiredConceptNameByIntent(intent, PUBLIC);
    }
    String getRequiredConceptNameByIntent(String intent, String namespace);


    default Relationship getRelationshipByIntent(String intent) {
        return getRelationshipByIntent(intent, PUBLIC);
    }
    Relationship getRelationshipByIntent(String intent, String namespace);

    default String getRelationshipNameByIntent(String intent) {
        return getRelationshipNameByIntent(intent, PUBLIC);
    }
    String getRelationshipNameByIntent(String intent, String namespace);

    /**
     * @deprecated With the addition of ontology sandboxing, ontology elements must now be retrieved with
     */
    @Deprecated
    default Relationship getRequiredRelationshipByIntent(String intent) {
        return getRequiredRelationshipByIntent(intent, PUBLIC);
    }
    Relationship getRequiredRelationshipByIntent(String intent, String namespace);


    default String getRequiredRelationshipNameByIntent(String intent) {
        return getRequiredRelationshipNameByIntent(intent, PUBLIC);
    }
    String getRequiredRelationshipNameByIntent(String intent, String namespace);


    default SchemaProperty getPropertyByIntent(String intent) {
        return getPropertyByIntent(intent, PUBLIC);
    }
    SchemaProperty getPropertyByIntent(String intent, String namespace);


    default String getPropertyNameByIntent(String intent) {
        return getPropertyNameByIntent(intent, PUBLIC);
    }
    String getPropertyNameByIntent(String intent, String namespace);

    default <T extends BcProperty> T getPropertyByIntent(String intent, Class<T> propertyType) {
        return getPropertyByIntent(intent, propertyType, PUBLIC);
    }
    <T extends BcProperty> T getPropertyByIntent(String intent, Class<T> propertyType, String namespace);

    default  <T extends BcProperty> T getRequiredPropertyByIntent(String intent, Class<T> propertyType) {
        return getRequiredPropertyByIntent(intent, propertyType, PUBLIC);
    }
    <T extends BcProperty> T getRequiredPropertyByIntent(String intent, Class<T> propertyType, String namespace);

    default List<SchemaProperty> getPropertiesByIntent(String intent) {
        return getPropertiesByIntent(intent, PUBLIC);
    }
    List<SchemaProperty> getPropertiesByIntent(String intent, String namespace);

    default SchemaProperty getRequiredPropertyByIntent(String intent) {
        return getRequiredPropertyByIntent(intent, PUBLIC);
    }
    SchemaProperty getRequiredPropertyByIntent(String intent, String namespace);

    default String getRequiredPropertyNameByIntent(String intent) {
        return getRequiredPropertyNameByIntent(intent, PUBLIC);
    }
    String getRequiredPropertyNameByIntent(String intent, String namespace);

    default SchemaProperty getDependentPropertyParent(String propertyName) {
        return getDependentPropertyParent(propertyName, PUBLIC);
    }
    SchemaProperty getDependentPropertyParent(String propertyName, String namespace);

    default void addConceptTypeFilterToQuery(BoolQueryBuilder query, String conceptName, boolean includeChildNodes) {
        addConceptTypeFilterToQuery(query, conceptName, includeChildNodes, PUBLIC);
    }
    void addConceptTypeFilterToQuery(BoolQueryBuilder query, String conceptName, boolean includeChildNodes, String namespace);

    default void addConceptTypeFilterToQuery(BoolQueryBuilder query, Collection<ElementTypeFilter> filters) {
        addConceptTypeFilterToQuery(query, filters, PUBLIC);
    }
    void addConceptTypeFilterToQuery(BoolQueryBuilder query, Collection<ElementTypeFilter> filters, String namespace);

    default void addEdgeLabelFilterToQuery(BoolQueryBuilder query, String edgeLabel, boolean includeChildNodes) {
        addEdgeLabelFilterToQuery(query, edgeLabel, includeChildNodes, PUBLIC);
    }
    void addEdgeLabelFilterToQuery(BoolQueryBuilder query, String edgeLabel, boolean includeChildNodes, String namespace);

    default void addEdgeLabelFilterToQuery(BoolQueryBuilder query, Collection<ElementTypeFilter> filters) {
        addEdgeLabelFilterToQuery(query, filters, PUBLIC);
    }
    void addEdgeLabelFilterToQuery(BoolQueryBuilder query, Collection<ElementTypeFilter> filters, String namespace);

    default void updatePropertyDependentNames(SchemaProperty property, Collection<String> dependentPropertyNames) {
        updatePropertyDependentNames(property, dependentPropertyNames, null, PUBLIC);
    }
    void updatePropertyDependentNames(SchemaProperty property, Collection<String> dependentPropertyNames, User user, String namespace);

    default void updatePropertyDomainNames(SchemaProperty property, Set<String> domainNames) {
        updatePropertyDomainNames(property, domainNames, null, PUBLIC);
    }
    void updatePropertyDomainNames(SchemaProperty property, Set<String> domainNames, User user, String namespace);

    String generateDynamicName(Class type, String displayName, String namespace, String... extended);

    String generatePropertyDynamicName(Class type, String displayName, String namespace, String prefix);

    void publishConcept(Concept concept, User user, String namespace);

    void publishRelationship(Relationship relationship, User user, String namespace);

    void publishProperty(SchemaProperty property, User user, String namespace);

    class ElementTypeFilter implements ClientApiObject {
        public String iri;
        public boolean includeChildNodes;

        public ElementTypeFilter() {
        }

        public ElementTypeFilter(String iri, boolean includeChildNodes) {
            this.iri = iri;
            this.includeChildNodes = includeChildNodes;
        }
    }

    void setIconProperty(
            Concept concept,
            File inDir,
            String glyphIconFileName,
            String propertyKey,
            User user,
            Authorizations authorizations
    ) throws IOException;

    Map<String,String> getVisibleProperties(String[] keepOntologyProps);

    Authorizations getAuthorizations();

    void getOrCreateInverseOfRelationship(
            Relationship fromRelationship,
            Relationship inverseOfRelationship
    );

    void removeInverseOfRelationship(
            Relationship fromRelationship,
            Relationship inverseOfRelationship
    );
}
