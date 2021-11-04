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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.cache.CacheOptions;
import com.mware.core.cache.CacheService;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcAccessDeniedException;
import com.mware.core.exception.BcException;
import com.mware.core.exception.BcResourceNotFoundException;
import com.mware.core.model.clientapi.dto.*;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.model.properties.types.BcProperty;
import com.mware.core.model.properties.types.BcPropertyBase;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.model.workspace.WorkspaceUser;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ExecutorServiceUtil;
import com.mware.ge.*;
import com.mware.ge.query.QueryResultsIterable;
import com.mware.ge.values.storable.*;
import com.mware.ge.query.GraphQuery;
import com.mware.ge.query.Query;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.util.StringUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.core.util.StreamUtil.stream;

public abstract class SchemaRepositoryBase implements SchemaRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SchemaRepositoryBase.class);
    private static final String ONTOLOGY_CACHE_NAME = SchemaRepository.class.getName() + ".ontology";
    private static final String ONTOLOGY_VISIBLEPROPS_CACHE_NAME = SchemaRepository.class.getName() + ".ontologyVisibleProps";
    private static final String CONFIG_ONTOLOGY_CACHE_MAX_SIZE = SchemaRepository.class.getName() + "ontologyCache.maxSize";
    private static final long CONFIG_ONTOLOGY_CACHE_MAX_SIZE_DEFAULT = 1000L;

    public static final String TOP_OBJECT_PROPERTY_NAME = "topObjectProperty";
    public static final int MAX_DISPLAY_NAME = 50;
    private final Configuration configuration;
    private final CacheService cacheService;
    private final CacheOptions ontologyCacheOptions;
    private PrivilegeRepository privilegeRepository;
    private WorkspaceRepository workspaceRepository;
    protected Authorizations authorizations;
    private final Graph graph;

    @Inject
    protected SchemaRepositoryBase(
            Configuration configuration,
            Graph graph,
            CacheService cacheService
    ) {
        this.configuration = configuration;
        this.cacheService = cacheService;
        this.ontologyCacheOptions = new CacheOptions()
                .setMaximumSize(configuration.getLong(CONFIG_ONTOLOGY_CACHE_MAX_SIZE, CONFIG_ONTOLOGY_CACHE_MAX_SIZE_DEFAULT))
                .setExpireAfterWrite(new Long(5));
        this.graph = graph;
    }

    public void loadOntologies() throws Exception {
        if (getThingConcept() == null) {
            Boolean simpleSchema = cacheService.getIfPresent("__cypherAcceptance", "simpleSchema");
            new DefaultSchemaCreator(this, simpleSchema != null ? simpleSchema : false).createOntology();
            clearCache();
        }

        new SchemaFactory(this)
                .applyContributions(configuration);
    }

    public Relationship getOrCreateRootRelationship(Authorizations authorizations) {
        User user = getSystemUser();
        Relationship topObjectProperty = internalGetOrCreateRelationshipType(
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                TOP_OBJECT_PROPERTY_NAME,
                null,
                true,
                true,
                user,
                PUBLIC
        );
        if (topObjectProperty.getUserVisible()) {
            topObjectProperty.setProperty(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.FALSE, user, authorizations);
        }
        return topObjectProperty;
    }

    public void addEntityGlyphIconToEntityConcept(Concept entityConcept, byte[] rawImg, boolean shouldFlush) {
        StreamingPropertyValue raw = new DefaultStreamingPropertyValue(new ByteArrayInputStream(rawImg), ByteArray.class);
        raw.searchIndex(false);
        entityConcept.setProperty(SchemaProperties.GLYPH_ICON.getPropertyName(), raw, getSystemUser(), getAuthorizations());

        if (shouldFlush)
            graph.flush();
    }

    @Override
    public void setIconProperty(
            Concept concept,
            File inDir,
            String glyphIconFileName,
            String propertyKey,
            User user,
            Authorizations authorizations
    ) throws IOException {
        if (glyphIconFileName != null) {
            File iconFile = new File(inDir, glyphIconFileName);
            if (!iconFile.exists()) {
                throw new RuntimeException("Could not find icon file: " + iconFile.toString());
            }
            try (InputStream iconFileIn = new FileInputStream(iconFile)) {
                StreamingPropertyValue value = new DefaultStreamingPropertyValue(iconFileIn, ByteArray.class);
                value.searchIndex(false);
                concept.setProperty(propertyKey, value, user, authorizations);
            }
        }
    }

    @Override
    public SchemaProperty getOrCreateProperty(SchemaPropertyDefinition schemaPropertyDefinition, User user, String namespace) {
        checkPrivileges(user, namespace);

        SchemaProperty property = getPropertyByName(schemaPropertyDefinition.getPropertyName(), namespace);
        if (property != null) {
            return property;
        }
        return addPropertyTo(
                schemaPropertyDefinition.getConcepts(),
                schemaPropertyDefinition.getRelationships(),
                schemaPropertyDefinition.getExtendedDataTableNames(),
                schemaPropertyDefinition.getPropertyName(),
                schemaPropertyDefinition.getDisplayName(),
                schemaPropertyDefinition.getDataType(),
                schemaPropertyDefinition.getPossibleValues(),
                schemaPropertyDefinition.getTextIndexHints(),
                schemaPropertyDefinition.isUserVisible(),
                schemaPropertyDefinition.isSearchFacet(),
                schemaPropertyDefinition.isSystemProperty(),
                schemaPropertyDefinition.getAggType(),
                schemaPropertyDefinition.getAggPrecision(),
                schemaPropertyDefinition.getAggInterval(),
                schemaPropertyDefinition.getAggMinDocumentCount(),
                schemaPropertyDefinition.getAggTimeZone(),
                schemaPropertyDefinition.getAggCalendarField(),
                schemaPropertyDefinition.isSearchable(),
                schemaPropertyDefinition.isAddable(),
                schemaPropertyDefinition.isSortable(),
                schemaPropertyDefinition.getSortPriority(),
                schemaPropertyDefinition.getDisplayType(),
                schemaPropertyDefinition.getPropertyGroup(),
                schemaPropertyDefinition.getBoost(),
                schemaPropertyDefinition.getValidationFormula(),
                schemaPropertyDefinition.getDisplayFormula(),
                schemaPropertyDefinition.getDependentPropertyNames(),
                schemaPropertyDefinition.getIntents(),
                schemaPropertyDefinition.getDeleteable(),
                schemaPropertyDefinition.getUpdateable(),
                user,
                namespace
        );
    }

    protected abstract SchemaProperty addPropertyTo(
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
            long aggMinDocumentCound,
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
    );

    @Override
    public Set<Concept> getConceptAndAllChildrenByName(String conceptName, String namespace) {
        Concept concept = getConceptByName(conceptName, namespace);
        if (concept == null) {
            return null;
        }
        return getConceptAndAllChildren(concept, namespace);
    }

    @Override
    public Set<Concept> getConceptAndAllChildren(Concept concept, String namespace) {
        List<Concept> childConcepts = getChildConcepts(concept, namespace);
        Set<Concept> result = Sets.newHashSet(concept);
        if (childConcepts.size() > 0) {
            List<Concept> childrenList = new ArrayList<>();
            for (Concept childConcept : childConcepts) {
                Set<Concept> child = getConceptAndAllChildren(childConcept, namespace);
                childrenList.addAll(child);
            }
            result.addAll(childrenList);
        }
        return result;
    }

    @Override
    public Set<Concept> getAncestorConcepts(Concept concept, String namespace) {
        Set<Concept> result = Sets.newHashSet();
        Concept parentConcept = getParentConcept(concept, namespace);
        while (parentConcept != null) {
            result.add(parentConcept);
            parentConcept = getParentConcept(parentConcept, namespace);
        }
        return result;
    }

    @Override
    public Set<Concept> getConceptAndAncestors(Concept concept, String namespace) {
        Set<Concept> result = Sets.newHashSet(concept);
        result.addAll(getAncestorConcepts(concept, namespace));
        return result;
    }

    public abstract List<Concept> getChildConcepts(Concept concept, String namespace);

    @Override
    public Set<Relationship> getRelationshipAndAllChildrenByName(String relationshipName, String namespace) {
        Relationship relationship = getRelationshipByName(relationshipName, namespace);
        return getRelationshipAndAllChildren(relationship, namespace);
    }

    @Override
    public Set<Relationship> getRelationshipAndAllChildren(Relationship relationship, String namespace) {
        List<Relationship> childRelationships = getChildRelationships(relationship, namespace);
        Set<Relationship> result = Sets.newHashSet(relationship);
        if (childRelationships.size() > 0) {
            List<Relationship> childrenList = new ArrayList<>();
            for (Relationship childRelationship : childRelationships) {
                Set<Relationship> child = getRelationshipAndAllChildren(childRelationship, namespace);
                childrenList.addAll(child);
            }
            result.addAll(childrenList);
        }
        return result;
    }

    @Override
    public Set<Relationship> getAncestorRelationships(Relationship relationship, String namespace) {
        Set<Relationship> result = Sets.newHashSet();
        Relationship parentRelationship = getParentRelationship(relationship, namespace);
        while (parentRelationship != null) {
            result.add(parentRelationship);
            parentRelationship = getParentRelationship(parentRelationship, namespace);
        }
        return result;
    }

    @Override
    public Set<Relationship> getRelationshipAndAncestors(Relationship relationship, String namespace) {
        Set<Relationship> result = Sets.newHashSet(relationship);
        result.addAll(getAncestorRelationships(relationship, namespace));
        return result;
    }

    @Override
    public boolean hasRelationshipByName(String relationshipName, String namespace) {
        return getRelationshipByName(relationshipName, namespace) != null;
    }

    protected abstract List<Relationship> getChildRelationships(Relationship relationship, String namespace);

    @Override
    public void resolvePropertyIds(JSONArray filterJson, String namespace) throws JSONException {
        for (int i = 0; i < filterJson.length(); i++) {
            JSONObject filter = filterJson.getJSONObject(i);
            if (filter.has("propertyId") && !filter.has("propertyName")) {
                String propertyVertexId = filter.getString("propertyId");
                SchemaProperty property = getPropertyByName(propertyVertexId, namespace);
                if (property == null) {
                    throw new RuntimeException("Could not find property with id: " + propertyVertexId);
                }
                filter.put("propertyName", property.getName());
                filter.put("propertyDataType", property.getDataType());
            }
        }
    }

    @Override
    public Concept getConceptByName(String conceptName, String namespace) {
        return Iterables.getFirst(getConceptsByName(Collections.singletonList(conceptName), namespace), null);
    }

    @Override
    public Iterable<Concept> getConceptsByName(List<String> conceptNames, String namespace) {
        return StreamSupport.stream(getConceptsWithProperties(namespace).spliterator(), false)
                .filter(concept -> conceptNames.contains(concept.getName()))
                .collect(Collectors.toSet());
    }

    @Override
    public boolean hasConceptByName(String conceptName, String namespace) {
        return getConceptByName(conceptName, namespace) != null;
    }

    @Override
    public SchemaProperty getPropertyByName(String propertyName, String namespace) {
        return Iterables.getFirst(getPropertiesByName(Collections.singletonList(propertyName), namespace), null);
    }

    @Override
    public Iterable<SchemaProperty> getPropertiesByName(List<String> propertyNames, String namespace) {
        return StreamSupport.stream(getProperties(namespace).spliterator(), false)
                .filter(property -> propertyNames.contains(property.getName()))
                .collect(Collectors.toList());
    }

    @Override
    public SchemaProperty getRequiredPropertyByName(String propertyName, String namespace) {
        SchemaProperty property = getPropertyByName(propertyName, namespace);
        if (property == null) {
            throw new BcException("Could not find property by name: " + propertyName);
        }
        return property;
    }

    @Override
    public Relationship getRelationshipByName(String relationshipName, String namespace) {
        return Iterables.getFirst(getRelationshipsByName(Collections.singletonList(relationshipName), namespace), null);
    }

    @Override
    public Iterable<Relationship> getRelationshipsByName(List<String> relationshipNames, String namespace) {
        return StreamSupport.stream(getRelationships(namespace).spliterator(), false)
                .filter(relationship -> relationshipNames.contains(relationship.getName()))
                .collect(Collectors.toList());
    }

    @Override
    public Concept getConceptByIntent(String intent, String namespace) {
        String configurationKey = CONFIG_INTENT_CONCEPT_PREFIX + intent;
        String conceptName = getConfiguration().get(configurationKey, null);
        if (conceptName != null) {
            Concept concept = getConceptByName(conceptName, namespace);
            if (concept == null) {
                throw new BcException("Could not find concept by configuration key: " + configurationKey);
            }
            return concept;
        }

        List<Concept> concepts = findLoadedConceptsByIntent(intent, namespace);
        if (concepts.size() == 0) {
            return null;
        }
        if (concepts.size() == 1) {
            return concepts.get(0);
        }

        String names = Joiner.on(',').join(new ConvertingIterable<Concept, String>(concepts) {
            @Override
            protected String convert(Concept o) {
                return o.getName();
            }
        });
        throw new BcException("Found multiple concepts for intent: " + intent + " (" + names + ")");
    }

    @Override
    public String getConceptNameByIntent(String intent, String namespace) {
        Concept concept = getConceptByIntent(intent, namespace);
        if (concept != null) {
            return concept.getName();
        }
        return null;
    }

    @Override
    public Concept getRequiredConceptByIntent(String intent, String namespace) {
        Concept concept = getConceptByIntent(intent, namespace);
        if (concept == null) {
            throw new BcException("Could not find concept by intent: " + intent);
        }
        return concept;
    }

    @Override
    public String getRequiredConceptNameByIntent(String intent, String namespace) {
        return getRequiredConceptByIntent(intent, namespace).getName();
    }

    @Override
    public Concept getRequiredConceptByName(String name, String namespace) {
        Concept concept = getConceptByName(name, namespace);
        if (concept == null) {
            throw new BcException("Could not find concept by name: " + name);
        }
        return concept;
    }

    @Override
    public String generateDynamicName(Class type, String displayName, String namespace, String... extended) {
        displayName = displayName.trim().replaceAll("\\s+", "_").toLowerCase();
        displayName = StringUtil.toCamelCase(displayName);
        displayName = displayName.substring(0, Math.min(displayName.length(), MAX_DISPLAY_NAME));
        String typeId = type.toString() + namespace + displayName;
        if (extended != null && extended.length > 0) {
            typeId += Joiner.on("").join(extended);
        }
        return displayName.replaceAll("[^a-zA-Z0-9_]", "")
                + "#"
                + Hashing.sha1().hashString(typeId, StandardCharsets.UTF_8).toString();
    }

    @Override
    public String generatePropertyDynamicName(Class type, String displayName, String namespace, String prefix){
        displayName = prefix + "_" + StringUtil.toCamelCase(displayName);
        displayName = displayName.substring(0, Math.min(displayName.length(), MAX_DISPLAY_NAME));
        return displayName.replaceAll("[^a-zA-Z0-9_]", "");
    }

    @Override
    public final Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, User user, String namespace) {
        return getOrCreateConcept(parent, conceptName, displayName, null, null, user, namespace);
    }

    @Override
    public final Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, String glyphIconHref, String color, User user, String namespace) {
        return getOrCreateConcept(parent, conceptName, displayName, glyphIconHref, color, true, false, user, namespace);
    }

    @Deprecated
    @Override
    public final Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, boolean deleteChangeableProperties, boolean isCoreConcept) {
        return getOrCreateConcept(parent, conceptName, displayName, deleteChangeableProperties, isCoreConcept, null, PUBLIC);
    }

    @Override
    public final Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, boolean deleteChangeableProperties, boolean isCoreConcept, User user, String namespace) {
        checkPrivileges(user, namespace);
        return internalGetOrCreateConcept(parent, conceptName, displayName, null, null, deleteChangeableProperties, isCoreConcept, user, namespace);
    }

    @Override
    public final Concept getOrCreateConcept(Concept parent, String conceptName, String displayName, String glyphIconHref, String color, boolean deleteChangeableProperties, boolean isCoreConcept, User user, String namespace) {
        checkPrivileges(user, namespace);
        return internalGetOrCreateConcept(parent, conceptName, displayName, glyphIconHref, color, deleteChangeableProperties, isCoreConcept, user, namespace);
    }

    protected abstract Concept internalGetOrCreateConcept(Concept parent, String conceptName, String displayName, String glyphIconHref, String color, boolean deleteChangeableProperties, boolean isCoreConcept, User user, String namespace);

    @Override
    public final Relationship getOrCreateRelationshipType(
            Relationship parent,
            Iterable<Concept> domainConcepts,
            Iterable<Concept> rangeConcepts,
            String relationshipName,
            boolean isDeclaredInOntology,
            boolean coreConcept,
            User user,
            String namespace
    ) {
        return getOrCreateRelationshipType(parent, domainConcepts, rangeConcepts, relationshipName, null, isDeclaredInOntology, coreConcept, user, namespace);
    }

    @Override
    public final Relationship getOrCreateRelationshipType(
            Relationship parent,
            Iterable<Concept> domainConcepts,
            Iterable<Concept> rangeConcepts,
            String relationshipName,
            String displayName,
            boolean isDeclaredInOntology,
            boolean coreConcept,
            User user,
            String namespace
    ) {
        checkPrivileges(user, namespace);
        if (parent == null && !relationshipName.equals(TOP_OBJECT_PROPERTY_NAME)) {
            parent = getTopObjectPropertyRelationship(namespace);
        }
        return internalGetOrCreateRelationshipType(parent, domainConcepts, rangeConcepts, relationshipName, displayName, isDeclaredInOntology, coreConcept, user, namespace);
    }

    protected abstract Relationship internalGetOrCreateRelationshipType(
            Relationship parent,
            Iterable<Concept> domainConcepts,
            Iterable<Concept> rangeConcepts,
            String relationshipName,
            String displayName,
            boolean isDeclaredInOntology,
            boolean coreConcept,
            User user,
            String namespace
    );

    @Override
    public Relationship getRelationshipByIntent(String intent, String namespace) {
        String configurationKey = CONFIG_INTENT_RELATIONSHIP_PREFIX + intent;
        String relationshipName = getConfiguration().get(configurationKey, null);
        if (relationshipName != null) {
            Relationship relationship = getRelationshipByName(relationshipName, namespace);
            if (relationship == null) {
                throw new BcException("Could not find relationship by configuration key: " + configurationKey);
            }
            return relationship;
        }

        List<Relationship> relationships = findLoadedRelationshipsByIntent(intent, namespace);
        if (relationships.size() == 0) {
            return null;
        }
        if (relationships.size() == 1) {
            return relationships.get(0);
        }

        String names = Joiner.on(',').join(new ConvertingIterable<Relationship, String>(relationships) {
            @Override
            protected String convert(Relationship o) {
                return o.getName();
            }
        });
        throw new BcException("Found multiple relationships for intent: " + intent + " (" + names + ")");
    }

    @Override
    public String getRelationshipNameByIntent(String intent, String namespace) {
        Relationship relationship = getRelationshipByIntent(intent, namespace);
        if (relationship != null) {
            return relationship.getName();
        }
        return null;
    }

    @Override
    public Relationship getRequiredRelationshipByIntent(String intent, String namespace) {
        Relationship relationship = getRelationshipByIntent(intent, namespace);
        if (relationship == null) {
            throw new BcException("Could not find relationship by intent: " + intent);
        }
        return relationship;
    }

    @Override
    public String getRequiredRelationshipNameByIntent(String intent, String namespace) {
        return getRequiredRelationshipByIntent(intent, namespace).getName();
    }

    @Override
    public SchemaProperty getPropertyByIntent(String intent, String namespace) {
        String configurationKey = CONFIG_INTENT_PROPERTY_PREFIX + intent;
        String propertyName = getConfiguration().get(configurationKey, null);
        if (propertyName != null) {
            SchemaProperty property = getPropertyByName(propertyName, namespace);
            if (property == null) {
                throw new BcException("Could not find property by configuration key: " + configurationKey);
            }
            return property;
        }

        List<SchemaProperty> properties = getPropertiesByIntent(intent, namespace);
        if (properties.size() == 0) {
            return null;
        }
        if (properties.size() == 1) {
            return properties.get(0);
        }

        String names = Joiner.on(',').join(new ConvertingIterable<SchemaProperty, String>(properties) {
            @Override
            protected String convert(SchemaProperty o) {
                return o.getName();
            }
        });
        throw new BcException("Found multiple properties for intent: " + intent + " (" + names + ")");
    }

    @Override
    public String getPropertyNameByIntent(String intent, String namespace) {
        SchemaProperty prop = getPropertyByIntent(intent, namespace);
        if (prop != null) {
            return prop.getName();
        }
        return null;
    }

    @Override
    public SchemaProperty getRequiredPropertyByIntent(String intent, String namespace) {
        SchemaProperty property = getPropertyByIntent(intent, namespace);
        if (property == null) {
            throw new BcException("Could not find property by intent: " + intent);
        }
        return property;
    }

    @Override
    public String getRequiredPropertyNameByIntent(String intent, String namespace) {
        return getRequiredPropertyByIntent(intent, namespace).getName();
    }

    @Override
    public SchemaProperty getDependentPropertyParent(String name, String namespace) {
        for (SchemaProperty property : getProperties(namespace)) {
            if (property.getDependentPropertyNames().contains(name)) {
                return property;
            }
        }
        return null;
    }

    @Override
    public <T extends BcProperty> T getPropertyByIntent(String intent, Class<T> propertyType, String namespace) {
        String propertyName = getPropertyNameByIntent(intent, namespace);
        if (propertyName == null) {
            LOGGER.warn("No property found for intent: %s", intent);
            return null;
        }
        try {
            Constructor<T> constructor = propertyType.getConstructor(String.class);
            return constructor.newInstance(propertyName);
        } catch (Exception ex) {
            throw new BcException("Could not create property for intent: " + intent + " (propertyName: " + propertyName + ")");
        }
    }

    @Override
    public <T extends BcProperty> T getRequiredPropertyByIntent(String intent, Class<T> propertyType, String namespace) {
        T result = getPropertyByIntent(intent, propertyType, namespace);
        if (result == null) {
            throw new BcException("Could not find property by intent: " + intent);
        }
        return result;
    }

    @Override
    public List<SchemaProperty> getPropertiesByIntent(String intent, String namespace) {
        List<SchemaProperty> results = new ArrayList<>();
        for (SchemaProperty property : getProperties(namespace)) {
            String[] propertyIntents = property.getIntents();
            if (Arrays.asList(propertyIntents).contains(intent)) {
                results.add(property);
            }
        }
        return results;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ClientApiSchema getClientApiObject(String namespace) {
        Schema schema = getOntology(namespace);
        Object[] results = ExecutorServiceUtil.runAllAndWait(
                () -> Concept.toClientApiConcepts(schema.getConcepts()),
                () -> SchemaProperty.toClientApiProperties(schema.getProperties()),
                () -> Relationship.toClientApiRelationships(schema.getRelationships())
        );

        ClientApiSchema clientOntology = new ClientApiSchema();
        clientOntology.addAllConcepts((Collection<ClientApiSchema.Concept>) results[0]);
        clientOntology.addAllProperties((Collection<ClientApiSchema.Property>) results[1]);
        clientOntology.addAllRelationships((Collection<ClientApiSchema.Relationship>) results[2]);
        return clientOntology;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Schema getOntology(String namespace) {
        if (namespace == null) {
            return getOntology(PUBLIC);
        }

        Schema schema = cacheService.getIfPresent(ONTOLOGY_CACHE_NAME, namespace);
        if (schema != null) {
            return schema;
        }

        Object[] results = ExecutorServiceUtil.runAllAndWait(
                () -> getConceptsWithProperties(namespace),
                () -> getRelationships(namespace),
                () -> getProperties(namespace)
        );

        Iterable<Concept> concepts = (Iterable<Concept>) results[0];
        Iterable<Relationship> relationships = (Iterable<Relationship>) results[1];
        Map<String, SchemaProperty> properties = stream((Iterable<SchemaProperty>) results[2])
                .collect(Collectors.toMap(SchemaProperty::getName, p -> p));

        List<ExtendedDataTableProperty> extendedDataTables = properties.values().stream()
                .filter(p -> p instanceof ExtendedDataTableProperty)
                .map(p -> (ExtendedDataTableProperty) p)
                .collect(Collectors.toList());

        schema = new Schema(
                concepts,
                relationships,
                extendedDataTables,
                properties,
                namespace
        );

        cacheService.put(ONTOLOGY_CACHE_NAME, namespace, schema, ontologyCacheOptions);
        return schema;
    }

    protected Relationship getTopObjectPropertyRelationship(String namespace) {
        return getRelationshipByName(TOP_OBJECT_PROPERTY_NAME, namespace);
    }

    @Override
    public void clearCache() {
        cacheService.invalidate(ONTOLOGY_CACHE_NAME);
        cacheService.invalidate(ONTOLOGY_VISIBLEPROPS_CACHE_NAME);
    }

    @Override
    public void clearCache(String namespace) {
        cacheService.invalidate(ONTOLOGY_CACHE_NAME, namespace);
    }

    public final Configuration getConfiguration() {
        return configuration;
    }

    protected void defineRequiredProperties(Graph graph) {
        definePropertyOnGraph(graph, BcSchema.MODIFIED_BY.getPropertyName(), StringValue.class, EnumSet.of(TextIndexHint.EXACT_MATCH), null, true);
        definePropertyOnGraph(graph, BcSchema.MODIFIED_DATE.getPropertyName(), DateTimeValue.class, TextIndexHint.NONE, null, true);
        definePropertyOnGraph(graph, BcSchema.VISIBILITY_JSON, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.ONTOLOGY_TITLE, StringValue.class, EnumSet.of(TextIndexHint.EXACT_MATCH));
        definePropertyOnGraph(graph, SchemaProperties.DISPLAY_NAME, StringValue.class, EnumSet.of(TextIndexHint.EXACT_MATCH));
        definePropertyOnGraph(graph, SchemaProperties.DISPLAY_TYPE, StringValue.class, EnumSet.of(TextIndexHint.EXACT_MATCH));
        definePropertyOnGraph(graph, SchemaProperties.INTENT, StringValue.class, EnumSet.of(TextIndexHint.EXACT_MATCH));
        definePropertyOnGraph(graph, SchemaProperties.TITLE_FORMULA, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.SUBTITLE_FORMULA, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.DISPLAY_FORMULA, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.TIME_FORMULA, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.GLYPH_ICON, ByteArray.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.MAP_GLYPH_ICON, ByteArray.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.GLYPH_ICON_FILE_NAME, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.DATA_TYPE, StringValue.class, EnumSet.of(TextIndexHint.EXACT_MATCH));
        definePropertyOnGraph(graph, SchemaProperties.USER_VISIBLE, BooleanValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.SEARCHABLE, BooleanValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.SEARCH_FACET, BooleanValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.SYSTEM_PROPERTY, BooleanValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.SORTABLE, BooleanValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.ADDABLE, BooleanValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.DELETEABLE, BooleanValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.UPDATEABLE, BooleanValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.TEXT_INDEX_HINTS, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.AGG_TYPE, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.CORE_CONCEPT, BooleanValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.COLOR, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.POSSIBLE_VALUES, StringValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.AGG_MIN_DOCUMENT_COUNT, LongValue.class, TextIndexHint.NONE);
        definePropertyOnGraph(graph, SchemaProperties.DEPENDENT_PROPERTY_ORDER_PROPERTY_NAME, IntValue.class, TextIndexHint.NONE);
    }

    protected void definePropertyOnGraph(Graph graph, BcPropertyBase<?> property, Class<? extends Value> dataType, Set<TextIndexHint> textIndexHint) {
        definePropertyOnGraph(graph, property.getPropertyName(), dataType, textIndexHint, null, false);
    }

    protected void definePropertyOnGraph(Graph graph, String propertyName, Class<? extends Value> dataType, Collection<TextIndexHint> textIndexHint, Double boost, boolean sortable) {
        if (!graph.isPropertyDefined(propertyName)) {
            DefinePropertyBuilder builder = graph.defineProperty(propertyName).dataType(dataType).sortable(sortable);
            if (textIndexHint != null) {
                builder.textIndexHint(textIndexHint);
            }
            if (boost != null) {
                if (graph.isFieldBoostSupported()) {
                    builder.boost(boost);
                } else {
                    LOGGER.warn("Field boosting is not support by the graph");
                }
            }
            builder.define();
        } else {
            PropertyDefinition propertyDefinition = graph.getPropertyDefinition(propertyName);
            if (propertyDefinition.getDataType() != dataType) {
                LOGGER.warn("Ontology property type mismatch for property %s! Expected %s but found %s",
                        propertyName, dataType.getName(), propertyDefinition.getDataType().getName());
            }
        }
    }

    protected boolean determineSearchable(
            String propertyNames,
            PropertyType dataType,
            Collection<TextIndexHint> textIndexHints,
            boolean searchable
    ) {
        if (dataType == PropertyType.EXTENDED_DATA_TABLE) {
            return false;
        }
        if (dataType == PropertyType.STRING) {
            checkNotNull(textIndexHints, "textIndexHints are required for string properties");
            if (searchable && (textIndexHints.isEmpty() || textIndexHints.equals(TextIndexHint.NONE))) {
                searchable = false;
            } else if (!searchable && (!textIndexHints.isEmpty() || !textIndexHints.equals(TextIndexHint.NONE))) {
                LOGGER.info("textIndexHints was specified for non-UI-searchable string property:: " + propertyNames);
            }
        }
        return searchable;
    }

    protected abstract Graph getGraph();

    protected abstract void internalDeleteConcept(Concept concept, String namespace);

    protected abstract void internalDeleteProperty(SchemaProperty property, String namespace);

    protected abstract void internalDeleteRelationship(Relationship relationship, String namespace);

    public void deleteConcept(String conceptTypeName, User user, String namespace) {
        checkDeletePrivileges(user, namespace);

        Set<Concept> concepts = getConceptAndAllChildrenByName(conceptTypeName, namespace);
        if (concepts.size() == 1) {
            for (Concept concept : concepts) {
                for (Relationship relationship : getRelationships(namespace)) {
                    if (relationship.getSourceConceptNames().contains(conceptTypeName) ||
                            relationship.getTargetConceptNames().contains(conceptTypeName)) {
                        throw new BcException("Unable to delete concept that is used in domain/range of relationship");
                    }
                }
                Graph graph = getGraph();
                Authorizations authorizations = graph.createAuthorizations(namespace);
                GraphQuery query = graph.query(authorizations);
                addConceptTypeFilterToQuery(query, concept.getName(), false, namespace);
                query.limit(0);

                QueryResultsIterable resultsIterable = query.search();
                long results = resultsIterable.getTotalHits();
                safeClose(resultsIterable);

                if (results == 0) {
                    List<SchemaProperty> removeProperties = concept.getProperties().stream().filter(ontologyProperty ->
                            ontologyProperty.getSandboxStatus().equals(SandboxStatus.PRIVATE) &&
                                    ontologyProperty.getRelationshipNames().size() == 0 &&
                                    ontologyProperty.getConceptNames().size() == 1 &&
                                    ontologyProperty.getConceptNames().get(0).equals(conceptTypeName)
                    ).collect(Collectors.toList());

                    internalDeleteConcept(concept, namespace);

                    for (SchemaProperty property : removeProperties) {
                        internalDeleteProperty(property, namespace);
                    }
                } else {
                    throw new BcException("Unable to delete concept that have vertices assigned to it");
                }
            }
        } else {
            throw new BcException("Unable to delete concept that have children");
        }

        clearCache();
    }

    private void safeClose(QueryResultsIterable iterable) {
        if (iterable == null) {
            return;
        }

        try {
            iterable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteProperty(String propertyName, User user, String namespace) {
        checkDeletePrivileges(user, namespace);

        SchemaProperty property = getPropertyByName(propertyName, namespace);
        if (property != null) {
            Graph graph = getGraph();
            Authorizations authorizations = graph.createAuthorizations(namespace);
            GraphQuery query = graph.query(authorizations);
            query.has(propertyName);
            query.limit(0);

            QueryResultsIterable resultsIterable = query.elementIds();
            long results = resultsIterable.getTotalHits();
            safeClose(resultsIterable);

            resultsIterable = query.extendedDataRowIds();
            results += resultsIterable.getTotalHits();
            safeClose(resultsIterable);

            if (results == 0) {
                internalDeleteProperty(property, namespace);
            } else {
                throw new BcException("Unable to delete property that have elements using it");
            }
        } else throw new BcResourceNotFoundException("Property not found");

        clearCache();
    }

    public void deleteRelationship(String relationshipName, User user, String namespace) {
        checkDeletePrivileges(user, namespace);

        Set<Relationship> relationships = getRelationshipAndAllChildrenByName(relationshipName, namespace);
        if (relationships.size() == 1) {
            for (Relationship relationship : relationships) {
                Graph graph = getGraph();
                Authorizations authorizations = graph.createAuthorizations(namespace);
                GraphQuery query = graph.query(authorizations);
                addEdgeLabelFilterToQuery(query, relationshipName, false, namespace);
                query.limit(0);

                QueryResultsIterable resultsIterable = query.search();
                long results = resultsIterable.getTotalHits();
                safeClose(resultsIterable);

                if (results == 0) {
                    List<SchemaProperty> removeProperties = relationship.getProperties().stream().filter(ontologyProperty ->
                            ontologyProperty.getSandboxStatus().equals(SandboxStatus.PRIVATE) &&
                                    ontologyProperty.getConceptNames().size() == 0 &&
                                    ontologyProperty.getRelationshipNames().size() == 1 &&
                                    ontologyProperty.getRelationshipNames().get(0).equals(relationshipName)
                    ).collect(Collectors.toList());
                    internalDeleteRelationship(relationship, namespace);

                    for (SchemaProperty property : removeProperties) {
                        internalDeleteProperty(property, namespace);
                    }
                } else {
                    throw new BcException("Unable to delete relationship that have edges using it");
                }
            }
        } else throw new BcException("Unable to delete relationship that have children");

        clearCache();
    }

    @Override
    public void addConceptTypeFilterToQuery(Query query, String conceptName, boolean includeChildNodes, String namespace) {
        checkNotNull(conceptName, "conceptName cannot be null");
        List<ElementTypeFilter> filters = new ArrayList<>();
        filters.add(new ElementTypeFilter(conceptName, includeChildNodes));
        addConceptTypeFilterToQuery(query, filters, namespace);
    }

    @Override
    public void addConceptTypeFilterToQuery(Query query, Collection<ElementTypeFilter> filters, String namespace) {
        checkNotNull(query, "query cannot be null");
        checkNotNull(filters, "filters cannot be null");

        if (filters.isEmpty()) {
            return;
        }

        Set<String> conceptIds = new HashSet<>(filters.size());

        for (ElementTypeFilter filter : filters) {
            Concept concept = getConceptByName(filter.iri, namespace);
            checkNotNull(concept, "Could not find concept with name: " + filter.iri);

            conceptIds.add(concept.getName());

            if (filter.includeChildNodes) {
                Set<Concept> childConcepts = getConceptAndAllChildren(concept, namespace);
                conceptIds.addAll(childConcepts.stream().map(Concept::getName).collect(Collectors.toSet()));
            }
        }

        query.hasConceptType(conceptIds);
    }

    @Override
    public void addEdgeLabelFilterToQuery(Query query, String edgeLabel, boolean includeChildNodes, String namespace) {
        checkNotNull(edgeLabel, "edgeLabel cannot be null");
        List<ElementTypeFilter> filters = new ArrayList<>();
        filters.add(new ElementTypeFilter(edgeLabel, includeChildNodes));
        addEdgeLabelFilterToQuery(query, filters, namespace);
    }

    @Override
    public void addEdgeLabelFilterToQuery(Query query, Collection<ElementTypeFilter> filters, String namespace) {
        checkNotNull(filters, "filters cannot be null");

        if (filters.isEmpty()) {
            return;
        }

        Set<String> edgeIds = new HashSet<>(filters.size());

        for (ElementTypeFilter filter : filters) {
            Relationship relationship = getRelationshipByName(filter.iri, namespace);
            checkNotNull(relationship, "Could not find edge with name: " + filter.iri);

            edgeIds.add(relationship.getName());

            if (filter.includeChildNodes) {
                Set<Relationship> childRelations = getRelationshipAndAllChildren(relationship, namespace);
                edgeIds.addAll(childRelations.stream().map(Relationship::getName).collect(Collectors.toSet()));
            }
        }

        query.hasEdgeLabel(edgeIds);
    }

    @Override
    public final void publishConcept(Concept concept, User user, String namespace) {
        checkPrivileges(user, null);
        internalPublishConcept(concept, user, namespace);
    }

    public abstract void internalPublishConcept(Concept concept, User user, String namespace);

    @Override
    public final void publishRelationship(Relationship relationship, User user, String namespace) {
        checkPrivileges(user, null);
        internalPublishRelationship(relationship, user, namespace);
    }

    public abstract void internalPublishRelationship(Relationship relationship, User user, String namespace);

    @Override
    public void publishProperty(SchemaProperty property, User user, String namespace) {
        checkPrivileges(user, null);
        internalPublishProperty(property, user, namespace);
    }

    public abstract void internalPublishProperty(SchemaProperty property, User user, String namespace);

    protected void checkPrivileges(User user, String namespace) {
        if (user != null && user.getUserType() == UserType.SYSTEM) {
            return;
        }

        if (user == null) {
            throw new BcAccessDeniedException("You must provide a valid user to perform this action", null, null);
        }

        if (isPublic(namespace)) {
            if (!getPrivilegeRepository().hasPrivilege(user, Privilege.ONTOLOGY_PUBLISH)) {
                throw new BcAccessDeniedException("User does not have ONTOLOGY_PUBLISH privilege", user, null);
            }
        } else {
            List<WorkspaceUser> users = getWorkspaceRepository().findUsersWithAccess(namespace, user);
            boolean access = users.stream()
                    .anyMatch(workspaceUser ->
                            workspaceUser.getUserId().equals(user.getUserId()) &&
                                    workspaceUser.getWorkspaceAccess().equals(WorkspaceAccess.WRITE));

            if (!access) {
                throw new BcAccessDeniedException("User does not have access to workspace", user, null);
            }

            if (!getPrivilegeRepository().hasPrivilege(user, Privilege.ONTOLOGY_ADD)) {
                throw new BcAccessDeniedException("User does not have ONTOLOGY_ADD privilege", user, null);
            }
        }
    }

    protected void checkDeletePrivileges(User user, String namespace) {
        if (user != null && user.getUserType() == UserType.SYSTEM) {
            return;
        }

        if (user == null) {
            throw new BcAccessDeniedException("You must provide a valid user to perform this action", null, null);
        }

        if (namespace == null) {
            throw new BcAccessDeniedException("User does not have access to delete published ontology items", user, null);
        } else if (!getPrivilegeRepository().hasPrivilege(user, Privilege.ADMIN)) {
            throw new BcAccessDeniedException("User does not have admin privilege", user, null);
        }
    }

    private List<Concept> findLoadedConceptsByIntent(String intent, String namespace) {
        List<Concept> results = new ArrayList<>();
        for (Concept concept : getConceptsWithProperties(namespace)) {
            String[] conceptIntents = concept.getIntents();
            if (Arrays.asList(conceptIntents).contains(intent)) {
                results.add(concept);
            }
        }
        return results;
    }

    private List<Relationship> findLoadedRelationshipsByIntent(String intent, String namespace) {
        List<Relationship> results = new ArrayList<>();
        for (Relationship relationship : getRelationships(namespace)) {
            String[] relationshipIntents = relationship.getIntents();
            if (Arrays.asList(relationshipIntents).contains(intent)) {
                results.add(relationship);
            }
        }
        return results;
    }

    protected User getSystemUser() {
        return new SystemUser();
    }

    protected PrivilegeRepository getPrivilegeRepository() {
        if (privilegeRepository == null) {
            privilegeRepository = InjectHelper.getInstance(PrivilegeRepository.class);
        }
        return privilegeRepository;
    }

    protected WorkspaceRepository getWorkspaceRepository() {
        if (workspaceRepository == null) {
            workspaceRepository = InjectHelper.getInstance(WorkspaceRepository.class);
        }
        return workspaceRepository;
    }

    protected abstract void deleteChangeableProperties(SchemaElement element, Authorizations authorizations);

    protected abstract void deleteChangeableProperties(SchemaProperty property, Authorizations authorizations);

    protected boolean isPublic(String worksapceId) {
        return worksapceId == null || PUBLIC.equals(worksapceId);
    }

    protected void validateRelationship(String relationshipName,
                                        Iterable<Concept> domainConcepts,
                                        Iterable<Concept> rangeConcepts) {
        if (!relationshipName.equals(TOP_OBJECT_PROPERTY_NAME)
                && (IterableUtils.isEmpty(domainConcepts) || IterableUtils.isEmpty(rangeConcepts))) {
            throw new BcException(relationshipName + " must have at least one domain and range ");
        }
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }

    public void setAuthorizations(Authorizations authorizations) {
        this.authorizations = authorizations;
    }

    @Override
    public Map<String, String> getVisibleProperties(String[] keepOntologyProps) {

        Map<String,String> visibleProperties = cacheService.getIfPresent(ONTOLOGY_VISIBLEPROPS_CACHE_NAME, PUBLIC);
        if (visibleProperties != null) {
            return visibleProperties;
        }
        visibleProperties = new HashMap<>();

        for (SchemaProperty property : getOntology(PUBLIC).getProperties()) {
            if (property == null)
                continue;

            String key = property.getName();
            String title = property.getName();
            boolean propertVisible = property.getUserVisible() || Arrays.asList(keepOntologyProps).contains(key);

            if (propertVisible && !visibleProperties.containsKey(key)) {
                visibleProperties.put(key, title);
            }
        }
        cacheService.put(ONTOLOGY_VISIBLEPROPS_CACHE_NAME, PUBLIC, visibleProperties, ontologyCacheOptions);

        return visibleProperties;
    }
}
