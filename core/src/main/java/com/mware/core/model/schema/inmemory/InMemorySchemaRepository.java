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
package com.mware.core.model.schema.inmemory;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.cache.CacheService;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.model.schema.*;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Authorizations;
import com.mware.ge.Graph;
import com.mware.ge.TextIndexHint;
import com.mware.ge.util.ConvertingIterable;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.ge.util.IterableUtils.toList;
import static com.mware.ge.values.storable.Values.stringValue;

@Singleton
public class InMemorySchemaRepository extends SchemaRepositoryBase {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(InMemorySchemaRepository.class);
    private static final String PUBLIC_ONTOLOGY_CACHE_KEY = "InMemoryOntologyRepository.PUBLIC";

    private final Graph graph;

    private final Map<String, Map<String, InMemoryConcept>> conceptsCache = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Map<String, InMemoryRelationship>> relationshipsCache = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Map<String, InMemorySchemaProperty>> propertiesCache = Collections.synchronizedMap(new HashMap<>());

    @Inject
    public InMemorySchemaRepository(
            Graph graph,
            Configuration configuration,
            CacheService cacheService
    ) throws Exception {
        super(configuration, graph, cacheService);
        this.graph = graph;

        clearCache();
        conceptsCache.put(PUBLIC_ONTOLOGY_CACHE_KEY, new HashMap<>());
        relationshipsCache.put(PUBLIC_ONTOLOGY_CACHE_KEY, new HashMap<>());
        propertiesCache.put(PUBLIC_ONTOLOGY_CACHE_KEY, new HashMap<>());

        loadOntologies();
    }

    private Map<String, InMemoryConcept> computeConceptCacheForWorkspace(String namespace) {
        Map<String, InMemoryConcept> workspaceConcepts = computeCacheForWorkspace(conceptsCache, namespace);

        if (!isPublic(namespace) && propertiesCache.containsKey(namespace)) {
            propertiesCache.get(namespace).values().forEach(workspaceProperty ->
                    workspaceProperty.getConceptNames().forEach(conceptName -> {
                        InMemoryConcept concept = workspaceConcepts.get(conceptName);
                        if (concept.getSandboxStatus() == SandboxStatus.PUBLIC) {
                            concept = concept.shallowCopy();
                            concept.getProperties().add(workspaceProperty);
                            workspaceConcepts.put(conceptName, concept);
                        }
                    }));
        }

        return workspaceConcepts;
    }

    private Map<String, InMemoryRelationship> computeRelationshipCacheForWorkspace(String namespace) {
        Map<String, InMemoryRelationship> workspaceRelationships = computeCacheForWorkspace(relationshipsCache, namespace);

        if (!isPublic(namespace) && propertiesCache.containsKey(namespace)) {
            propertiesCache.get(namespace).values().forEach(workspaceProperty ->
                    workspaceProperty.getRelationshipNames().forEach(relationshipName -> {
                        InMemoryRelationship relationship = workspaceRelationships.get(relationshipName);
                        if (relationship.getSandboxStatus() == SandboxStatus.PUBLIC) {
                            relationship = relationship.shallowCopy();
                            relationship.getProperties().add(workspaceProperty);
                            workspaceRelationships.put(relationshipName, relationship);
                        }
                    }));
        }

        return workspaceRelationships;
    }

    private Map<String, InMemorySchemaProperty> computePropertyCacheForWorkspace(String namespace) {
        return computeCacheForWorkspace(propertiesCache, namespace);
    }

    private <T> Map<String, T> computeCacheForWorkspace(Map<String, Map<String, T>> cache, String namespace) {
        Map<String, T> result = new HashMap<>();
        result.putAll(cache.compute(PUBLIC_ONTOLOGY_CACHE_KEY, (k, v) -> v == null ? new HashMap<>() : v));
        if (!isPublic(namespace) && cache.containsKey(namespace)) {
            result.putAll(cache.get(namespace));
        }
        return result;
    }

    @Override
    public void updatePropertyDependentNames(SchemaProperty property, Collection<String> dependentPropertyNames, User user, String namespace) {
        if (!isPublic(namespace) || property.getSandboxStatus() == SandboxStatus.PRIVATE) {
            throw new UnsupportedOperationException("Sandboxed updating of dependent names is not currently supported for properties");
        }

        InMemorySchemaProperty inMemoryOntologyProperty = (InMemorySchemaProperty) property;
        inMemoryOntologyProperty.setDependentPropertyNames(dependentPropertyNames);
    }

    @Override
    public void addDomainConceptsToRelationshipType(String relationshipName, List<String> conceptNames, User user, String namespace) {
        InMemoryRelationship relationship = computeRelationshipCacheForWorkspace(namespace).get(relationshipName);
        if (!isPublic(namespace) && relationship.getSandboxStatus() != SandboxStatus.PRIVATE) {
            throw new UnsupportedOperationException("Sandboxed updating of domain names is not currently supported for published relationships");
        }

        List<String> missingConcepts = conceptNames.stream()
                .filter(c -> !relationship.getSourceConceptNames().contains(c))
                .collect(Collectors.toList());

        if (relationship.getSandboxStatus() == SandboxStatus.PRIVATE) {
            relationship.getSourceConceptNames().addAll(missingConcepts);
        } else {
            InMemoryRelationship inMemoryRelationship = relationship.shallowCopy();
            inMemoryRelationship.getSourceConceptNames().addAll(missingConcepts);

            Map<String, InMemoryRelationship> workspaceCache = relationshipsCache.compute(namespace, (k, v) -> v == null ? new HashMap<>() : v);
            workspaceCache.put(inMemoryRelationship.getName(), inMemoryRelationship);
        }
    }

    @Override
    public void addRangeConceptsToRelationshipType(String relationshipName, List<String> conceptNames, User user, String namespace) {
        InMemoryRelationship relationship = computeRelationshipCacheForWorkspace(namespace).get(relationshipName);
        if (!isPublic(namespace) && relationship.getSandboxStatus() != SandboxStatus.PRIVATE) {
            throw new UnsupportedOperationException("Sandboxed updating of range names is not currently supported for published relationships");
        }

        List<String> missingConcepts = conceptNames.stream()
                .filter(c -> !relationship.getTargetConceptNames().contains(c))
                .collect(Collectors.toList());

        if (relationship.getSandboxStatus() == SandboxStatus.PRIVATE) {
            relationship.getTargetConceptNames().addAll(missingConcepts);
        } else {
            InMemoryRelationship inMemoryRelationship = relationship.shallowCopy();
            inMemoryRelationship.getTargetConceptNames().addAll(missingConcepts);

            Map<String, InMemoryRelationship> workspaceCache = relationshipsCache.compute(namespace, (k, v) -> v == null ? new HashMap<>() : v);
            workspaceCache.put(inMemoryRelationship.getName(), inMemoryRelationship);
        }
    }

    @Override
    public SchemaProperty addPropertyTo(
            List<Concept> concepts,
            List<Relationship> relationships,
            List<String> extendedDataTableNames,
            String propertyName,
            String displayName,
            PropertyType dataType,
            Map<String, String> possibleValues,
            Collection<TextIndexHint> textIndexHints,
            boolean userVisible,
            boolean searchFacet,
            boolean systemProperty,
            String aggType,
            int aggPrecision,
            String aggInterval,
            long aggMinDocumentCount,
            String aggTimeZone,
            String aggCalendarField,
            boolean searchable,
            boolean addable,
            boolean sortable,
            Integer sortPriority,
            String displayType,
            String propertyGroup,
            Double boost,
            String validationFormula,
            String displayFormula,
            ImmutableList<String> dependentPropertyNames,
            String[] intents,
            boolean deleteable,
            boolean updateable,
            User user,
            String namespace
    ) {
        checkNotNull(concepts, "concept was null");
        InMemorySchemaProperty property = getPropertyByName(propertyName, namespace);
        if (property == null) {
            searchable = determineSearchable(propertyName, dataType, textIndexHints, searchable);
            definePropertyOnGraph(graph, propertyName, PropertyType.getTypeClass(dataType), textIndexHints, boost, sortable);

            if (dataType.equals(PropertyType.EXTENDED_DATA_TABLE)) {
                property = new InMemoryExtendedDataTableSchemaProperty();
            } else {
                property = new InMemorySchemaProperty();
            }
            property.setDataType(dataType);
        } else {
            deleteChangeableProperties(property, null);
        }

        property.setUserVisible(userVisible);
        property.setSearchable(searchable);
        property.setAddable(addable);
        property.setSortable(sortable);
        property.setSortPriority(sortPriority);
        property.setName(propertyName);
        property.setBoost(boost);
        property.setDisplayType(displayType);
        property.setPropertyGroup(propertyGroup);
        property.setValidationFormula(validationFormula);
        property.setDisplayFormula(displayFormula);
        property.setDeleteable(deleteable);
        property.setUpdateable(updateable);
        property.setWorkspaceId(isPublic(namespace) ? null : namespace);
        if (dependentPropertyNames != null && !dependentPropertyNames.isEmpty()) {
            property.setDependentPropertyNames(dependentPropertyNames);
        }
        if (intents != null) {
            for (String intent : intents) {
                property.addIntent(intent);
            }
        }
        if (displayName != null && !displayName.trim().isEmpty()) {
            property.setDisplayName(displayName);
        }
        if (textIndexHints != null && textIndexHints.size() > 0) {
            for (TextIndexHint textIndexHint : textIndexHints) {
                property.addTextIndexHints(textIndexHint);
            }
        }
        property.setPossibleValues(possibleValues);

        String cacheKey = isPublic(namespace) ? PUBLIC_ONTOLOGY_CACHE_KEY : namespace;
        Map<String, InMemorySchemaProperty> workspaceCache = propertiesCache.compute(cacheKey, (k, v) -> v == null ? new HashMap<>() : v);
        workspaceCache.put(propertyName, property);

        for (Concept concept : concepts) {
            property.getConceptNames().add(concept.getName());
            if (isPublic(namespace) || concept.getSandboxStatus() == SandboxStatus.PRIVATE) {
                concept.getProperties().add(property);
            }
        }

        for (Relationship relationship : relationships) {
            property.getRelationshipNames().add(relationship.getName());
            if (isPublic(namespace) || relationship.getSandboxStatus() == SandboxStatus.PRIVATE) {
                relationship.getProperties().add(property);
            }
        }

        if (extendedDataTableNames != null) {
            for (String extendedDataTableName : extendedDataTableNames) {
                InMemoryExtendedDataTableSchemaProperty edtp = (InMemoryExtendedDataTableSchemaProperty) getPropertyByName(extendedDataTableName, namespace);
                edtp.addTableProperty(property.getName());
            }
        }

        checkNotNull(property, "Could not find property: " + propertyName);
        return property;
    }

    @Override
    public void updatePropertyDomainNames(SchemaProperty property, Set<String> domainNames, User user, String namespace) {
        if (!isPublic(namespace) && property.getSandboxStatus() != SandboxStatus.PRIVATE) {
            throw new UnsupportedOperationException("Sandboxed updating of domain names is not currently supported for published properties");
        }

        InMemorySchemaProperty inMemoryProperty = (InMemorySchemaProperty) property;

        for (Concept concept : getConceptsWithProperties(namespace)) {
            if (concept.getProperties().contains(property)) {
                if (!domainNames.remove(concept.getName())) {
                    if (isPublic(namespace) || concept.getSandboxStatus() == SandboxStatus.PRIVATE) {
                        concept.getProperties().remove(property);
                    }
                    inMemoryProperty.getConceptNames().remove(concept.getName());
                }
            }
        }
        for (Relationship relationship : getRelationships(namespace)) {
            if (relationship.getProperties().contains(property)) {
                if (!domainNames.remove(relationship.getName())) {
                    if (isPublic(namespace) || relationship.getSandboxStatus() == SandboxStatus.PRIVATE) {
                        relationship.getProperties().remove(property);
                    }
                    inMemoryProperty.getRelationshipNames().remove(relationship.getName());
                }
            }
        }

        for (String domainName : domainNames) {
            InMemoryConcept concept = getConceptByName(domainName, namespace);
            if (concept != null) {
                if (isPublic(namespace) || concept.getSandboxStatus() == SandboxStatus.PRIVATE) {
                    concept.getProperties().add(property);
                }
                inMemoryProperty.getConceptNames().add(concept.getName());
            } else {
                InMemoryRelationship relationship = getRelationshipByName(domainName, namespace);
                if (relationship != null) {
                    if (isPublic(namespace) || relationship.getSandboxStatus() == SandboxStatus.PRIVATE) {
                        relationship.getProperties().add(property);
                    }
                    inMemoryProperty.getRelationshipNames().add(relationship.getName());
                } else {
                    throw new BcException("Could not find domain with name " + domainName);
                }
            }
        }
    }

    @Override
    public void internalPublishConcept(Concept concept, User user, String namespace) {
        if (conceptsCache.containsKey(namespace)) {
            Map<String, InMemoryConcept> sandboxedConcepts = conceptsCache.get(namespace);
            if (sandboxedConcepts.containsKey(concept.getName())) {
                InMemoryConcept sandboxConcept = sandboxedConcepts.remove(concept.getName());
                sandboxConcept.removeWorkspaceId();
                conceptsCache.get(PUBLIC_ONTOLOGY_CACHE_KEY).put(concept.getName(), sandboxConcept);
            }
        }
    }

    @Override
    public void internalPublishRelationship(Relationship relationship, User user, String namespace) {
        if (relationshipsCache.containsKey(namespace)) {
            Map<String, InMemoryRelationship> sandboxedRelationships = relationshipsCache.get(namespace);
            if (sandboxedRelationships.containsKey(relationship.getName())) {
                InMemoryRelationship sandboxRelationship = sandboxedRelationships.remove(relationship.getName());
                sandboxRelationship.removeWorkspaceId();
                relationshipsCache.get(PUBLIC_ONTOLOGY_CACHE_KEY).put(relationship.getName(), sandboxRelationship);
            }
        }
    }

    @Override
    public void internalPublishProperty(SchemaProperty property, User user, String namespace) {
        if (propertiesCache.containsKey(namespace)) {
            Map<String, InMemorySchemaProperty> sandboxedProperties = propertiesCache.get(namespace);
            if (sandboxedProperties.containsKey(property.getName())) {
                InMemorySchemaProperty sandboxProperty = sandboxedProperties.remove(property.getName());
                sandboxProperty.removeWorkspaceId();
                propertiesCache.get(PUBLIC_ONTOLOGY_CACHE_KEY).put(property.getName(), sandboxProperty);

                Map<String, InMemoryConcept> publicConcepts = conceptsCache.get(PUBLIC_ONTOLOGY_CACHE_KEY);
                sandboxProperty.getConceptNames().forEach(c -> publicConcepts.get(c).getProperties().add(sandboxProperty));

                Map<String, InMemoryRelationship> publicRelationships = relationshipsCache.get(PUBLIC_ONTOLOGY_CACHE_KEY);
                sandboxProperty.getRelationshipNames().forEach(r -> publicRelationships.get(r).getProperties().add(sandboxProperty));
            }
        }
    }

    @Override
    public void getOrCreateInverseOfRelationship(Relationship fromRelationship, Relationship inverseOfRelationship) {
        InMemoryRelationship fromRelationshipMem = (InMemoryRelationship) fromRelationship;
        InMemoryRelationship inverseOfRelationshipMem = (InMemoryRelationship) inverseOfRelationship;

        fromRelationshipMem.addInverseOf(inverseOfRelationshipMem);
        inverseOfRelationshipMem.addInverseOf(fromRelationshipMem);
    }

    @Override
    public void removeInverseOfRelationship(Relationship fromRelationship, Relationship inverseOfRelationship) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Iterable<Relationship> getRelationships(String namespace) {
        return new ArrayList<>(computeRelationshipCacheForWorkspace(namespace).values());
    }

    @Override
    public Iterable<Relationship> getRelationships(Iterable<String> ids, String namespace) {
        if (ids != null) {
            List<String> idList = toList(ids);
            Iterable<Relationship> workspaceRelationships = getRelationships(namespace);
            return StreamSupport.stream(workspaceRelationships.spliterator(), true)
                    .filter(r -> idList.contains(r.getId()))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public Iterable<SchemaProperty> getProperties(Iterable<String> ids, String namespace) {
        if (ids != null) {
            List<String> idList = toList(ids);
            Iterable<SchemaProperty> workspaceProps = getProperties(namespace);
            return StreamSupport.stream(workspaceProps.spliterator(), true)
                    .filter(p -> idList.contains(p.getId()))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public Iterable<SchemaProperty> getProperties(String namespace) {
        return new ArrayList<>(computePropertyCacheForWorkspace(namespace).values());
    }

    @Override
    public String getDisplayNameForLabel(String relationshipName, String namespace) {
        InMemoryRelationship relationship = computeRelationshipCacheForWorkspace(namespace).get(relationshipName);
        checkNotNull(relationship, "Could not find relationship " + relationshipName);
        return relationship.getDisplayName();
    }

    @Override
    public InMemorySchemaProperty getPropertyByName(String propertyName, String namespace) {
        return computePropertyCacheForWorkspace(namespace).get(propertyName);
    }

    @Override
    public InMemoryRelationship getRelationshipByName(String relationshipName, String namespace) {
        return computeRelationshipCacheForWorkspace(namespace).get(relationshipName);
    }

    @Override
    public InMemoryConcept getConceptByName(String conceptName, String namespace) {
        return computeConceptCacheForWorkspace(namespace).get(conceptName);
    }

    @Override
    public boolean hasRelationshipByName(String relationshipName, String namespace) {
        return getRelationshipByName(relationshipName, namespace) != null;
    }

    @Override
    public Iterable<Concept> getConceptsWithProperties(String namespace) {
        return new ArrayList<>(computeConceptCacheForWorkspace(namespace).values());
    }

    @Override
    public Concept getRootConcept(String namespace) {
        return computeConceptCacheForWorkspace(namespace).get(InMemorySchemaRepository.ROOT_CONCEPT_NAME);
    }

    @Override
    public Concept getThingConcept(String namespace) {
        return computeConceptCacheForWorkspace(namespace).get(InMemorySchemaRepository.THING_CONCEPT_NAME);
    }

    @Override
    public Concept getParentConcept(Concept concept, String namespace) {
        return computeConceptCacheForWorkspace(namespace).get(concept.getParentConceptName());
    }

    @Override
    public Relationship getParentRelationship(Relationship relationship, String namespace) {
        return computeRelationshipCacheForWorkspace(namespace).get(relationship.getParentName());
    }

    @Override
    public Iterable<Concept> getConcepts(Iterable<String> ids, String namespace) {
        if (ids != null) {
            List<String> idList = toList(ids);
            Iterable<Concept> workspaceConcepts = getConceptsWithProperties(namespace);
            return StreamSupport.stream(workspaceConcepts.spliterator(), true)
                    .filter(c -> idList.contains(c.getId()))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    protected void internalDeleteConcept(Concept concept, String namespace) {
        String cacheKey = namespace;
        Map<String, InMemoryConcept> workspaceCache = conceptsCache.compute(cacheKey, (k, v) -> v == null ? new HashMap<>() : v);
        workspaceCache.remove(concept.getName());

        for (SchemaProperty property : getProperties(namespace)) {
            property.getConceptNames().remove(concept.getName());
        }
    }

    @Override
    protected void internalDeleteProperty(SchemaProperty property, String namespace) {
        String cacheKey = namespace;
        Map<String, InMemorySchemaProperty> workspaceCache = propertiesCache.compute(cacheKey, (k, v) -> v == null ? new HashMap<>() : v);
        workspaceCache.remove(property.getName());
    }

    @Override
    protected void internalDeleteRelationship(Relationship relationship, String namespace) {
        String cacheKey = namespace;
        Map<String, InMemoryRelationship> workspaceCache = relationshipsCache.compute(cacheKey, (k, v) -> v == null ? new HashMap<>() : v);
        workspaceCache.remove(relationship.getName());

        for (SchemaProperty property : getProperties(namespace)) {
            property.getRelationshipNames().remove(relationship.getName());
        }
    }

    @Override
    public List<Concept> getChildConcepts(Concept concept, String namespace) {
        Map<String, InMemoryConcept> workspaceConcepts = computeConceptCacheForWorkspace(namespace);
        return workspaceConcepts.values().stream()
                .filter(workspaceConcept -> concept.getName().equals(workspaceConcept.getParentConceptName()))
                .collect(Collectors.toList());
    }

    @Override
    protected List<Relationship> getChildRelationships(Relationship relationship, String namespace) {
        Map<String, InMemoryRelationship> workspaceRelationships = computeRelationshipCacheForWorkspace(namespace);
        return workspaceRelationships.values().stream()
                .filter(workspaceRelationship -> relationship.getName().equals(workspaceRelationship.getParentName()))
                .collect(Collectors.toList());
    }

    @Override
    protected Concept internalGetOrCreateConcept(Concept parent, String conceptName, String displayName, String glyphIconHref, String color, boolean deleteChangeableProperties, boolean isCoreConcept, User user, String namespace) {
        InMemoryConcept concept = getConceptByName(conceptName, namespace);

        if (concept != null) {
            if (deleteChangeableProperties) {
                deleteChangeableProperties(concept, null);
            }
            return concept;
        }
        if (parent == null) {
            concept = new InMemoryConcept(conceptName, null, isPublic(namespace) ? null : namespace);
        } else {
            concept = new InMemoryConcept(conceptName, parent.getName(), isPublic(namespace) ? null : namespace);
        }
        concept.setProperty(SchemaProperties.TITLE.getPropertyName(), stringValue(conceptName), user, null);
        concept.setProperty(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue(displayName), user, null);

        if (conceptName.equals(SchemaRepository.THING_CONCEPT_NAME)) {
            concept.setProperty(SchemaProperties.TITLE_FORMULA.getPropertyName(), stringValue("prop('title') || ''"), user, null);

            // TODO: change to ontology && ontology.displayName
            concept.setProperty(SchemaProperties.SUBTITLE_FORMULA.getPropertyName(), stringValue("prop('source') || ''"), user, null);
            concept.setProperty(SchemaProperties.TIME_FORMULA.getPropertyName(), stringValue("''"), user, null);
        }

        if (!StringUtils.isEmpty(glyphIconHref)) {
            concept.setProperty(SchemaProperties.GLYPH_ICON_FILE_NAME.getPropertyName(), stringValue(glyphIconHref), user, null);
        }
        if (!StringUtils.isEmpty(color)) {
            concept.setProperty(SchemaProperties.COLOR.getPropertyName(), stringValue(color), user, null);
        }

        String cacheKey = isPublic(namespace) ? PUBLIC_ONTOLOGY_CACHE_KEY : namespace;
        Map<String, InMemoryConcept> workspaceCache = conceptsCache.compute(cacheKey, (k, v) -> v == null ? new HashMap<>() : v);
        workspaceCache.put(conceptName, concept);

        return concept;
    }

    @Override
    protected Relationship internalGetOrCreateRelationshipType(
            Relationship parent,
            Iterable<Concept> domainConcepts,
            Iterable<Concept> rangeConcepts,
            String relationshipName,
            String displayName,
            boolean deleteChangeableProperties,
            boolean coreConcept,
            User user,
            String namespace
    ) {
        Relationship relationship = getRelationshipByName(relationshipName, namespace);
        if (relationship != null) {
            if (deleteChangeableProperties) {
                deleteChangeableProperties(relationship, null);
            }

            for (Concept domainConcept : domainConcepts) {
                if (!relationship.getSourceConceptNames().contains(domainConcept.getName())) {
                    relationship.getSourceConceptNames().add(domainConcept.getName());
                }
            }

            for (Concept rangeConcept : rangeConcepts) {
                if (!relationship.getTargetConceptNames().contains(rangeConcept.getName())) {
                    relationship.getTargetConceptNames().add(rangeConcept.getName());
                }
            }

            return relationship;
        }

        validateRelationship(relationshipName, domainConcepts, rangeConcepts);

        List<String> domainConceptNames = toList(new ConvertingIterable<Concept, String>(domainConcepts) {
            @Override
            protected String convert(Concept o) {
                return o.getName();
            }
        });

        List<String> rangeConceptNames = toList(new ConvertingIterable<Concept, String>(rangeConcepts) {
            @Override
            protected String convert(Concept o) {
                return o.getName();
            }
        });

        String parentName = parent == null ? null : parent.getName();
        Collection<SchemaProperty> properties = new ArrayList<>();
        InMemoryRelationship inMemRelationship = new InMemoryRelationship(
                parentName,
                relationshipName,
                domainConceptNames,
                rangeConceptNames,
                properties,
                isPublic(namespace) ? null : namespace
        );

        if (displayName != null) {
            inMemRelationship.setProperty(SchemaProperties.DISPLAY_NAME.getPropertyName(), stringValue(displayName), user, getAuthorizations(namespace));
        }

        String cacheKey = isPublic(namespace) ? PUBLIC_ONTOLOGY_CACHE_KEY : namespace;
        Map<String, InMemoryRelationship> workspaceCache = relationshipsCache.compute(cacheKey, (k, v) -> v == null ? new HashMap<>() : v);
        workspaceCache.put(relationshipName, inMemRelationship);

        return inMemRelationship;
    }

    protected Authorizations getAuthorizations(String namespace, String... otherAuthorizations) {
        if (isPublic(namespace) && (otherAuthorizations == null || otherAuthorizations.length == 0)) {
            return new Authorizations(VISIBILITY_STRING);
        }

        if (isPublic(namespace)) {
            return new Authorizations(ArrayUtils.add(otherAuthorizations, VISIBILITY_STRING));
        } else if (otherAuthorizations == null || otherAuthorizations.length == 0) {
            return new Authorizations(VISIBILITY_STRING, namespace);
        }
        return new Authorizations(ArrayUtils.addAll(otherAuthorizations, VISIBILITY_STRING, namespace));
    }

    protected Graph getGraph() {
        return graph;
    }

    @Override
    protected void deleteChangeableProperties(SchemaProperty property, Authorizations authorizations) {
        for (String propertyName : SchemaProperties.CHANGEABLE_PROPERTY_NAME) {
            if (SchemaProperties.INTENT.getPropertyName().equals(propertyName)) {
                for (String intent : property.getIntents()) {
                    property.removeIntent(intent, null);
                }
            } else {
                property.setProperty(propertyName, null, null, null);
            }
        }
    }

    @Override
    protected void deleteChangeableProperties(SchemaElement element, Authorizations authorizations) {
        for (String propertyName : SchemaProperties.CHANGEABLE_PROPERTY_NAME) {
            if (element instanceof InMemoryRelationship) {
                InMemoryRelationship inMemoryRelationship = (InMemoryRelationship) element;
                if (SchemaProperties.INTENT.getPropertyName().equals(propertyName)) {
                    for (String intent : inMemoryRelationship.getIntents()) {
                        inMemoryRelationship.removeIntent(intent, null);
                    }
                } else {
                    inMemoryRelationship.removeProperty(propertyName, null);
                }
            } else {
                InMemoryConcept inMemoryConcept = (InMemoryConcept) element;
                if (SchemaProperties.INTENT.getPropertyName().equals(propertyName)) {
                    for (String intent : inMemoryConcept.getIntents()) {
                        inMemoryConcept.removeIntent(intent, null);
                    }
                } else {
                    inMemoryConcept.removeProperty(propertyName, null);
                }
            }
        }
    }
}

