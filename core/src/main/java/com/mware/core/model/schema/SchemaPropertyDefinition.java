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
import com.mware.ge.TextIndexHint;
import com.mware.core.model.clientapi.dto.PropertyType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SchemaPropertyDefinition {
    private final List<Concept> concepts;
    private final List<Relationship> relationships;
    private List<String> extendedDataTableNames;
    private final String propertyName;
    private final String displayName;
    private final PropertyType dataType;
    private Map<String, String> possibleValues;
    private Collection<TextIndexHint> textIndexHints;
    private Collection<String> extendedDataTableDomains;
    private boolean userVisible = true;
    private boolean analyticsVisible;
    private boolean searchFacet;
    private String aggType;
    private int aggPrecision = -1;
    private String aggInterval;
    private long aggMinDocumentCount = 0L;
    private String aggTimeZone;
    private String aggCalendarField;
    private boolean systemProperty = false;
    private boolean searchable = true;
    private boolean addable = true;
    private boolean sortable = true;
    private Integer sortPriority;
    private String displayType;
    private String propertyGroup;
    private Double boost;
    private String validationFormula;
    private String displayFormula;
    private ImmutableList<String> dependentPropertyNames;
    private String[] intents;
    private boolean deleteable = true;
    private boolean updateable = true;

    public SchemaPropertyDefinition(
            List<Concept> concepts,
            String propertyName,
            String displayName,
            PropertyType dataType
    ) {
        this(concepts, new ArrayList<>(), propertyName, displayName, dataType);
    }

    public SchemaPropertyDefinition(
            List<Concept> concepts,
            List<Relationship> relationships,
            String propertyName,
            String displayName,
            PropertyType dataType
    ) {
        this.concepts = concepts;
        this.relationships = relationships;
        this.propertyName = propertyName;
        this.displayName = displayName;
        this.dataType = dataType;
    }

    public List<Relationship> getRelationships() {
        return relationships;
    }

    public List<Concept> getConcepts() {
        return concepts;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public PropertyType getDataType() {
        return dataType;
    }

    public Map<String, String> getPossibleValues() {
        return possibleValues;
    }

    public SchemaPropertyDefinition setPossibleValues(Map<String, String> possibleValues) {
        this.possibleValues = possibleValues;
        return this;
    }

    public Collection<TextIndexHint> getTextIndexHints() {
        return textIndexHints;
    }

    public SchemaPropertyDefinition setTextIndexHints(Collection<TextIndexHint> textIndexHints) {
        this.textIndexHints = textIndexHints;
        return this;
    }

    public List<String> getExtendedDataTableNames() {
        return extendedDataTableNames;
    }

    public SchemaPropertyDefinition setExtendedDataTableDomain(List<String> extendedDataTableNames) {
        this.extendedDataTableNames = extendedDataTableNames;
        return this;
    }

    public boolean isUserVisible() {
        return userVisible;
    }

    public boolean isSearchFacet() {
        return searchFacet;
    }

    public String getAggType() {
        return aggType;
    }

    public int getAggPrecision() {
        return aggPrecision;
    }

    public String getAggInterval() {
        return aggInterval;
    }

    public long getAggMinDocumentCount() {
        return aggMinDocumentCount;
    }

    public String getAggTimeZone() {
        return aggTimeZone;
    }

    public String getAggCalendarField() {
        return aggCalendarField;
    }

    public SchemaPropertyDefinition setUserVisible(boolean userVisible) {
        this.userVisible = userVisible;
        return this;
    }

    public SchemaPropertyDefinition setSearchFacet(boolean searchFacet) {
        this.searchFacet = searchFacet;
        return this;
    }

    public SchemaPropertyDefinition setAggType(String aggType) {
        this.aggType = aggType;
        return this;
    }

    public SchemaPropertyDefinition setAggPrecision(int aggPrecision) {
        this.aggPrecision = aggPrecision;
        return this;
    }

    public SchemaPropertyDefinition setAggInterval(String aggInterval) {
        this.aggInterval = aggInterval;
        return this;
    }

    public SchemaPropertyDefinition setAggMinDocumentCount(long aggMinDocumentCount) {
        this.aggMinDocumentCount = aggMinDocumentCount;
        return this;
    }

    public SchemaPropertyDefinition setAggTimeZone(String aggTimeZone) {
        this.aggTimeZone = aggTimeZone;
        return this;
    }

    public SchemaPropertyDefinition setAggCalendarField(String aggCalendarField) {
        this.aggCalendarField = aggCalendarField;
        return this;
    }

    public boolean isSystemProperty() {
        return systemProperty;
    }

    public SchemaPropertyDefinition setSystemProperty(boolean systemProperty) {
        this.systemProperty = systemProperty;
        return this;
    }

    public boolean isSearchable() {
        return searchable;
    }

    public SchemaPropertyDefinition setSearchable(boolean searchable) {
        this.searchable = searchable;
        return this;
    }

    public boolean isAddable() {
        return addable;
    }

    public SchemaPropertyDefinition setAddable(boolean addable) {
        this.addable = addable;
        return this;
    }

    public boolean isSortable() {
        return sortable;
    }

    public SchemaPropertyDefinition setSortable(boolean sortable) {
        this.sortable = sortable;
        return this;
    }

    public Integer getSortPriority() {
        return sortPriority;
    }

    public SchemaPropertyDefinition setSortPriority(Integer sortPriority) {
        this.sortPriority = sortPriority;
        return this;
    }

    public String getDisplayType() {
        return displayType;
    }

    public SchemaPropertyDefinition setDisplayType(String displayType) {
        this.displayType = displayType;
        return this;
    }

    public String getPropertyGroup() {
        return propertyGroup;
    }

    public SchemaPropertyDefinition setPropertyGroup(String propertyGroup) {
        this.propertyGroup = propertyGroup;
        return this;
    }

    public Double getBoost() {
        return boost;
    }

    public SchemaPropertyDefinition setBoost(Double boost) {
        this.boost = boost;
        return this;
    }

    public String getValidationFormula() {
        return validationFormula;
    }

    public SchemaPropertyDefinition setValidationFormula(String validationFormula) {
        this.validationFormula = validationFormula;
        return this;
    }

    public String getDisplayFormula() {
        return displayFormula;
    }

    public SchemaPropertyDefinition setDisplayFormula(String displayFormula) {
        this.displayFormula = displayFormula;
        return this;
    }

    public ImmutableList<String> getDependentPropertyNames() {
        return dependentPropertyNames;
    }

    public SchemaPropertyDefinition setDependentPropertyNames(ImmutableList<String> dependentPropertyNames) {
        this.dependentPropertyNames = dependentPropertyNames;
        return this;
    }

    public String[] getIntents() {
        return intents;
    }

    public SchemaPropertyDefinition setIntents(String[] intents) {
        this.intents = intents;
        return this;
    }

    public boolean getDeleteable() {
        return deleteable;
    }

    public void setDeleteable(boolean deleteable) {
        this.deleteable = deleteable;
    }

    public boolean getUpdateable() {
        return updateable;
    }

    public void setUpdateable(boolean updateable) {
        this.updateable = updateable;
    }
}
