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

import com.google.common.collect.ImmutableList;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ServiceLoaderUtil;
import com.mware.ge.Authorizations;
import com.mware.ge.TextIndexHint;
import com.mware.ge.collection.Iterables;
import com.mware.ge.values.storable.BooleanValue;
import com.mware.ge.values.storable.Value;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.mware.core.model.schema.SchemaRepository.PUBLIC;

public class SchemaFactory {
    public static final String RESOURCE_ENTITY_PNG = "entity.png";
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SchemaFactory.class);

    private SchemaRepositoryBase schemaRepository;
    private Authorizations authorizations;
    private User systemUser = new SystemUser();
    private String workspaceId = PUBLIC;

    public SchemaFactory(SchemaRepository schemaRepository) {
        this.authorizations = schemaRepository.getAuthorizations();
        this.schemaRepository = (SchemaRepositoryBase) schemaRepository;
    }

    public Relationship getOrCreateRootRelationship() {
        return schemaRepository.getOrCreateRootRelationship(authorizations);
    }

    public SchemaFactory forNamespace(String namespace) {
        if (!StringUtils.isEmpty(namespace))
            this.workspaceId = namespace;

        return this;
    }

    public Concept getOrCreateThingConcept() {
        Concept thingConcept = schemaRepository.getConceptByName(SchemaConstants.CONCEPT_TYPE_THING);
        if (thingConcept == null) {
            thingConcept = newConcept()
                    .conceptType(SchemaConstants.CONCEPT_TYPE_THING)
                    .displayName("Thing")
                    .property(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.TRUE)
                    .coreConcept(true)
                    .save();
        }
        return thingConcept;
    }

    public DefaultConcept newConcept() {
        return new DefaultConcept();
    }

    public DefaultRelationship newRelationship() {
        return new DefaultRelationship();
    }

    public DefaultConceptProperty newConceptProperty() {
        return new DefaultConceptProperty();
    }

    private void addDefaultIcon(Concept entityConcept) {
        addGlyphIcon(entityConcept, SchemaRepositoryBase.class.getResourceAsStream(RESOURCE_ENTITY_PNG));
    }

    private void addGlyphIcon(Concept entityConcept, InputStream icon) {
        if (entityConcept.getGlyphIcon() != null) {
            LOGGER.debug("entityConcept GlyphIcon already set. skipping addEntityGlyphIcon.");
            return;
        }
        checkNotNull(icon, "The icon input stream is null");

        try {
            ByteArrayOutputStream imgOut = new ByteArrayOutputStream();
            IOUtils.copy(icon, imgOut);
            byte[] rawImg = imgOut.toByteArray();
            com.mware.ge.io.IOUtils.closeAllSilently(icon, imgOut);
            schemaRepository.addEntityGlyphIconToEntityConcept(entityConcept, rawImg, true);
        } catch (IOException e) {
            throw new BcException("invalid stream for glyph icon");
        }
    }

    public Concept getConcept(String name) {
        return schemaRepository.getConceptByName(name, workspaceId);
    }

    public Relationship getRelationship(String name) {
        return schemaRepository.getRelationshipByName(name, workspaceId);
    }

    public SchemaProperty getProperty(String name) {
        return schemaRepository.getPropertyByName(name, workspaceId);
    }

    public void applyContributions(Configuration configuration) {
        try {
            Iterables.stream(ServiceLoaderUtil.loadWithoutInjecting(SchemaContribution.class, configuration))
                    .filter(sc -> !sc.patchApplied(this))
                    .forEach(sc -> sc.patchSchema(this));
        } catch (Exception ex) {
            LOGGER.warn("Could not apply schema contributions", ex);
        }
    }

    public SchemaRepository getSchemaRepository() {
        return schemaRepository;
    }

    public class DefaultConcept {
        private Concept parent;
        private String conceptType;
        private String displayName;
        private Map<String, Value> properties = new HashMap<>();
        private String[] intents;
        private String glyphIcon;
        private InputStream glyphIconStream;
        private boolean coreConcept = false;

        public DefaultConcept() {
            properties.put(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.FALSE);
        }

        public DefaultConcept parent(Concept parentConcept) {
            this.parent = parentConcept;
            return this;
        }

        public DefaultConcept conceptType(String conceptType) {
            this.conceptType = conceptType;
            return this;
        }

        public DefaultConcept displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public DefaultConcept glyphIcon(String glyphIcon) {
            this.glyphIcon = glyphIcon;
            return this;
        }

        public DefaultConcept glyphIcon(InputStream glyphIconStream) {
            this.glyphIconStream = glyphIconStream;
            return this;
        }

        public DefaultConcept property(String name, Value value) {
            properties.put(name, value);
            return this;
        }

        public DefaultConcept intents(String... intents) {
            this.intents = intents;
            return this;
        }

        public DefaultConcept coreConcept(boolean coreConcept) {
            this.coreConcept = coreConcept;
            return this;
        }

        public Concept save() {
            if (parent == null && !SchemaConstants.CONCEPT_TYPE_THING.equals(conceptType))
                parent = getOrCreateThingConcept();

            Concept c = schemaRepository.getOrCreateConcept(parent, conceptType, displayName, true, coreConcept, systemUser, workspaceId);
            for (Map.Entry<String, Value> prop : properties.entrySet()) {
                c.setProperty(prop.getKey(), prop.getValue(), new SystemUser(), authorizations);
            }

            if (intents != null) {
                Arrays.stream(intents).forEach(intent -> c.addIntent(intent, new SystemUser(), authorizations));
            }

            if (!StringUtils.isEmpty(glyphIcon))
                addGlyphIcon(c, SchemaRepositoryBase.class.getResourceAsStream(glyphIcon));
            else if (glyphIconStream != null)
                addGlyphIcon(c, glyphIconStream);
            else
                addDefaultIcon(c);

            return c;
        }
    }

    public class DefaultRelationship {
        private Relationship parent;
        private Concept[] sourceConcepts;
        private Concept[] targetConcepts;
        private Relationship inverseOf;
        private String label;
        private Map<String, Value> properties = new HashMap<>();
        private String[] intents;
        private boolean coreConcept = false;

        public DefaultRelationship() {
            properties.put(SchemaProperties.USER_VISIBLE.getPropertyName(), BooleanValue.FALSE);
        }

        public DefaultRelationship parent(Relationship parentRelationship) {
            this.parent = parentRelationship;
            return this;
        }

        public DefaultRelationship label(String label) {
            this.label = label;
            return this;
        }

        public DefaultRelationship source(Concept... sourceConcepts) {
            this.sourceConcepts = sourceConcepts;
            return this;
        }

        public DefaultRelationship target(Concept... targetConcepts) {
            this.targetConcepts = targetConcepts;
            return this;
        }

        public DefaultRelationship property(String name, Value value) {
            properties.put(name, value);
            return this;
        }

        public DefaultRelationship intents(String... intents) {
            this.intents = intents;
            return this;
        }

        public DefaultRelationship inverseOf(Relationship inverseOf) {
            this.inverseOf = inverseOf;
            return this;
        }

        public DefaultRelationship coreConcept(boolean coreConcept) {
            this.coreConcept = coreConcept;
            return this;
        }

        public Relationship save() {
            if (parent == null)
                parent = getOrCreateRootRelationship();

            Relationship r = schemaRepository.getOrCreateRelationshipType(parent, Arrays.asList(sourceConcepts), Arrays.asList(targetConcepts), label, true, coreConcept, systemUser, workspaceId);

            for (Map.Entry<String, Value> prop : properties.entrySet()) {
                r.setProperty(prop.getKey(), prop.getValue(), new SystemUser(), authorizations);
            }
            if (intents != null) {
                Arrays.stream(intents).forEach(intent -> r.addIntent(intent, new SystemUser(), authorizations));
            }

            if (inverseOf != null)
                schemaRepository.getOrCreateInverseOfRelationship(r, inverseOf);

            return r;
        }
    }

    public class DefaultConceptProperty {
        private Concept[] concepts;
        private String name;
        private String displayName = "";
        private String displayFormula;
        private String validationFormula;
        private boolean userVisible = false;
        private boolean searchFacet = false;
        private boolean searchable = true;
        private boolean addable = true;
        private boolean updatable = true;
        private boolean deletable = true;
        private boolean sortable = true;
        private boolean systemProperty = true;
        private String aggType;
        private long aggMinDocumentCount = 0L;
        private Collection<TextIndexHint> textIndexHints = TextIndexHint.NONE;
        private PropertyType type;
        private String displayType;
        private String propertyGroup;
        private String[] intents;
        private Map<String, String> possibleValues;
        private Relationship[] forRelationships;
        private ImmutableList<String> dependentPropertyNames;
        private ImmutableList<String> extendedDataTableNames;

        public DefaultConceptProperty() {
        }

        public DefaultConceptProperty concepts(Concept... concepts) {
            this.concepts = concepts;
            return this;
        }

        public DefaultConceptProperty name(String name) {
            this.name = name;
            return this;
        }

        public DefaultConceptProperty displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public DefaultConceptProperty displayType(String displayType) {
            this.displayType = displayType;
            return this;
        }

        public DefaultConceptProperty propertyGroup(String propertyGroup) {
            this.propertyGroup = propertyGroup;
            return this;
        }

        public DefaultConceptProperty displayFormula(String displayFormula) {
            this.displayFormula = displayFormula;
            return this;
        }

        public DefaultConceptProperty validationFormula(String validationFormula) {
            this.validationFormula = validationFormula;
            return this;
        }

        public DefaultConceptProperty userVisible(boolean userVisible) {
            this.userVisible = userVisible;
            return this;
        }

        public DefaultConceptProperty searchFacet(boolean searchFacet) {
            this.searchFacet = searchFacet;
            return this;
        }

        public DefaultConceptProperty sortable(boolean sortable) {
            this.sortable = sortable;
            return this;
        }

        public DefaultConceptProperty searchable(boolean searchable) {
            this.searchable = searchable;
            return this;
        }

        public DefaultConceptProperty addable(boolean addable) {
            this.addable = addable;
            return this;
        }

        public DefaultConceptProperty updatable(boolean updatable) {
            this.updatable = updatable;
            return this;
        }

        public DefaultConceptProperty deletable(boolean deletable) {
            this.deletable = deletable;
            return this;
        }

        public DefaultConceptProperty systemProperty(boolean systemProperty) {
            this.systemProperty = systemProperty;
            return this;
        }

        public DefaultConceptProperty aggType(String aggType) {
            this.aggType = aggType;
            return this;
        }

        public DefaultConceptProperty aggMinDocumentCount(long aggMinDocumentCount) {
            this.aggMinDocumentCount = aggMinDocumentCount;
            return this;
        }

        public DefaultConceptProperty type(PropertyType type) {
            this.type = type;
            return this;
        }

        public DefaultConceptProperty possibleValues(Map<String, String> possibleValues) {
            this.possibleValues = possibleValues;
            return this;
        }

        public DefaultConceptProperty textIndexHints(Collection<TextIndexHint> textIndexHints) {
            this.textIndexHints = textIndexHints;
            return this;
        }

        public DefaultConceptProperty textIndexHints(TextIndexHint... textIndexHints) {
            this.textIndexHints = Arrays.asList(textIndexHints);
            return this;
        }

        public DefaultConceptProperty forRelationships(Relationship... relationships) {
            this.forRelationships = relationships;
            return this;
        }

        public DefaultConceptProperty intents(String... intents) {
            this.intents = intents;
            return this;
        }

        public DefaultConceptProperty dependentPropertyNames(ImmutableList<String> dependentPropertyNames) {
            this.dependentPropertyNames = dependentPropertyNames;
            return this;
        }

        public DefaultConceptProperty extendedDataTableNames(ImmutableList<String> extendedDataTableNames) {
            this.extendedDataTableNames = extendedDataTableNames;
            return this;
        }

        public SchemaProperty save() {
            if (concepts == null || concepts.length == 0) {
                concepts = new Concept[]{SchemaFactory.this.getOrCreateThingConcept()};
            }
            SchemaPropertyDefinition schemaPropertyDefinition = new SchemaPropertyDefinition(Arrays.asList(concepts), name, displayName, type);
            schemaPropertyDefinition.setUserVisible(userVisible);
            schemaPropertyDefinition.setTextIndexHints(textIndexHints);
            schemaPropertyDefinition.setSearchFacet(searchFacet);
            schemaPropertyDefinition.setAggType(aggType);
            schemaPropertyDefinition.setAggMinDocumentCount(aggMinDocumentCount);
            schemaPropertyDefinition.setDisplayType(displayType);
            schemaPropertyDefinition.setSortable(sortable);
            schemaPropertyDefinition.setPossibleValues(possibleValues);
            schemaPropertyDefinition.setSystemProperty(systemProperty);
            schemaPropertyDefinition.setSearchable(searchable);
            schemaPropertyDefinition.setAddable(addable);
            schemaPropertyDefinition.setUpdateable(updatable);
            schemaPropertyDefinition.setDeleteable(deletable);
            schemaPropertyDefinition.setDependentPropertyNames(dependentPropertyNames);
            schemaPropertyDefinition.setDisplayFormula(displayFormula);
            schemaPropertyDefinition.setValidationFormula(validationFormula);
            schemaPropertyDefinition.setPropertyGroup(propertyGroup);
            schemaPropertyDefinition.setExtendedDataTableDomain(extendedDataTableNames);

            if (forRelationships != null)
                schemaPropertyDefinition.getRelationships().addAll(Arrays.asList(forRelationships));

            SchemaProperty prop = schemaRepository.addPropertyTo(
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
                    systemUser,
                    workspaceId
            );


            if (intents != null) {
                Arrays.stream(intents).forEach(intent -> prop.addIntent(intent, authorizations));
            }

            return prop;
        }
    }
}
