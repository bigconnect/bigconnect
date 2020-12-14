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
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.values.storable.*;

import java.util.*;

public class InMemorySchemaProperty extends SchemaProperty {
    private String name;
    private boolean userVisible;
    private boolean searchable;
    private boolean sortable;
    private boolean addable;
    private String displayName;
    private String propertyGroup;
    private PropertyType dataType;
    private Map<String, String> possibleValues;
    private String displayType;
    private Double boost;
    private String validationFormula;
    private String displayFormula;
    private boolean updateable;
    private boolean deleteable;
    private Integer sortPriority;
    private boolean searchFacet;
    private boolean systemProperty;
    private String aggType;
    private int aggPrecision;
    private String aggInterval;
    private long aggMinDocumentCount;
    private String aggTimeZone;
    private String aggCalendarField;
    private ImmutableList<String> dependentPropertyNames = ImmutableList.of();
    private List<String> intents = new ArrayList<>();
    private List<String> textIndexHints = new ArrayList<>();
    private Map<String, String> metadata = new HashMap<>();

    private List<String> conceptNames = new ArrayList<>();
    private List<String> relationshipNames = new ArrayList<>();

    private String workspaceId;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getId() {
        return name;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public boolean getUserVisible() {
        return userVisible;
    }

    @Override
    public PropertyType getDataType() {
        return dataType;
    }

    @Override
    public Double getBoost() {
        return boost;
    }

    @Override
    public Map<String, String> getPossibleValues() {
        return possibleValues;
    }

    @Override
    public String getPropertyGroup() {
        return propertyGroup;
    }

    @Override
    public boolean getSearchable() {
        return searchable;
    }

    @Override
    public boolean getSearchFacet() {
        return false;
    }

    @Override
    public boolean getSystemProperty() {
        return false;
    }

    @Override
    public boolean getAddable() {
        return addable;
    }

    @Override
    public boolean getSortable() {
        return sortable;
    }

    @Override
    public Integer getSortPriority() {
        return sortPriority;
    }

    @Override
    public String getAggType() {
        return aggType;
    }

    @Override
    public int getAggPrecision() {
        return aggPrecision;
    }

    @Override
    public String getAggInterval() {
        return aggInterval;
    }

    @Override
    public long getAggMinDocumentCount() {
        return aggMinDocumentCount;
    }

    @Override
    public String getAggTimeZone() {
        return aggTimeZone;
    }

    @Override
    public String getAggCalendarField() {
        return aggCalendarField;
    }

    @Override
    public String getValidationFormula() {
        return validationFormula;
    }

    @Override
    public String getDisplayFormula() {
        return displayFormula;
    }

    @Override
    public ImmutableList<String> getDependentPropertyNames() {
        return dependentPropertyNames;
    }

    @Override
    public boolean getDeleteable() {
        return deleteable;
    }

    @Override
    public boolean getUpdateable() {
        return updateable;
    }

    public String[] getIntents() {
        return this.intents.toArray(new String[this.intents.size()]);
    }

    @Override
    public String[] getTextIndexHints() {
        return this.textIndexHints.toArray(new String[this.textIndexHints.size()]);
    }

    @Override
    public void addTextIndexHints(String textIndexHints, Authorizations authorizations) {
        addTextIndexHints(textIndexHints);
    }

    public void addTextIndexHints(String textIndexHints) {
        this.textIndexHints.add(textIndexHints);
    }

    public String getDisplayType() {
        return displayType;
    }

    public void setSearchable(boolean searchable) {
        this.searchable = searchable;
    }

    public void setAddable(boolean addable) {
        this.addable = addable;
    }

    public void setSortable(boolean sortable) {
        this.sortable = sortable;
    }

    public void setSortPriority(Integer sortPriority) {
        this.sortPriority = sortPriority;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setUserVisible(boolean userVisible) {
        this.userVisible = userVisible;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public void setDataType(PropertyType dataType) {
        this.dataType = dataType;
    }

    public void setPossibleValues(Map<String, String> possibleValues) {
        this.possibleValues = possibleValues;
    }

    public void setBoost(Double boost) {
        this.boost = boost;
    }

    public void setDisplayType(String displayType) {
        this.displayType = displayType;
    }

    public void setPropertyGroup(String propertyGroup) {
        this.propertyGroup = propertyGroup;
    }

    public void setValidationFormula(String validationFormula) {
        this.validationFormula = validationFormula;
    }

    public void setDisplayFormula(String displayFormula) {
        this.displayFormula = displayFormula;
    }

    public void setDependentPropertyNames(Collection<String> dependentPropertyNames) {
        this.dependentPropertyNames = dependentPropertyNames == null ? ImmutableList.<String>of() : ImmutableList.copyOf(dependentPropertyNames);
    }

    public void setUpdateable(boolean updateable) {
        this.updateable = updateable;
    }

    public void setDeleteable(boolean deleteable) {
        this.deleteable = deleteable;
    }

    @Override
    public void setProperty(String name, Value value, User user, Authorizations authorizations) {
        if (SchemaProperties.DISPLAY_TYPE.getPropertyName().equals(name)) {
            this.displayType = ((TextValue) value).stringValue();
        } else if (SchemaProperties.DISPLAY_FORMULA.getPropertyName().equals(name)) {
            this.displayFormula = ((TextValue) value).stringValue();
        } else if (SchemaProperties.DISPLAY_NAME.getPropertyName().equals(name)) {
            this.displayName = ((TextValue) value).stringValue();
        } else if (SchemaProperties.PROPERTY_GROUP.getPropertyName().equals(name)) {
            this.propertyGroup = ((TextValue) value).stringValue();
        } else if (SchemaProperties.SEARCHABLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.searchable = ((BooleanValue) value).booleanValue();
            } else {
                this.searchable = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (SchemaProperties.SORTABLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.sortable = ((BooleanValue) value).booleanValue();
            } else {
                this.sortable = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (SchemaProperties.SORT_PRIORITY.getPropertyName().equals(name)) {
            if (value == null) {
                this.sortPriority = null;
            } else if (value instanceof IntValue) {
                this.sortPriority = ((IntValue) value).value();
            } else {
                this.sortPriority = Integer.parseInt(((TextValue) value).stringValue());
            }
        } else if (SchemaProperties.ADDABLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.addable = ((BooleanValue) value).booleanValue();
            } else {
                this.addable = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (SchemaProperties.USER_VISIBLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.userVisible = ((BooleanValue) value).booleanValue();
            } else {
                this.userVisible = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (SchemaProperties.DELETEABLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.deleteable = ((BooleanValue) value).booleanValue();
            } else {
                this.deleteable = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (SchemaProperties.UPDATEABLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.updateable = ((BooleanValue) value).booleanValue();
            } else {
                this.updateable = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (value != null) {
            this.metadata.put(name, value.toString());
        } else {
            this.metadata.remove(name);
        }
    }

    public void addIntent(String intent) {
        this.intents.add(intent);
    }

    @Override
    public void addIntent(String intent, Authorizations authorizations) {
        this.intents.add(intent);
    }

    @Override
    public void removeIntent(String intent, Authorizations authorizations) {
        this.intents.remove(intent);
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public List<String> getConceptNames() {
        return conceptNames;
    }

    @Override
    public List<String> getRelationshipNames() {
        return relationshipNames;
    }

    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getWorkspaceId() {
        return workspaceId;
    }

    public void removeWorkspaceId() {
        workspaceId = null;
    }

    @Override
    public SandboxStatus getSandboxStatus() {
        return workspaceId == null ? SandboxStatus.PUBLIC : SandboxStatus.PRIVATE;
    }

}
