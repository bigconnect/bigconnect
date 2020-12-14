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

import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.Relationship;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.values.storable.BooleanValue;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;

import java.util.*;

public class InMemoryRelationship extends Relationship {
    private String name;
    private String displayName;
    private List<Relationship> inverseOfs = new ArrayList<>();
    private Set<String> intents = new HashSet<>();
    private boolean userVisible = true;
    private boolean deleteable;
    private boolean updateable;
    private boolean coreConcept;
    private String titleFormula;
    private String subtitleFormula;
    private String timeFormula;
    private String workspaceId;
    private String color;
    private Map<String, String> metadata = new HashMap<>();

    protected InMemoryRelationship(
            String parentName,
            String name,
            List<String> domainConceptNames,
            List<String> rangeConceptNames,
            Collection<SchemaProperty> properties,
            String workspaceId
    ) {
        super(parentName, domainConceptNames, rangeConceptNames, properties);
        this.name = name;
        this.workspaceId = workspaceId;
    }

    InMemoryRelationship shallowCopy() {
        InMemoryRelationship other = new InMemoryRelationship(
                getParentName(),
                getName(),
                new ArrayList<>(),
                new ArrayList<>(),
                new ArrayList<>(),
                workspaceId);
        other.getSourceConceptNames().addAll(getSourceConceptNames());
        other.getTargetConceptNames().addAll(getTargetConceptNames());
        other.getProperties().addAll(getProperties());

        other.displayName = displayName;
        other.inverseOfs.addAll(inverseOfs);
        other.intents.addAll(intents);
        other.userVisible = userVisible;
        other.deleteable = deleteable;
        other.updateable = updateable;
        other.titleFormula = titleFormula;
        other.subtitleFormula = subtitleFormula;
        other.timeFormula = timeFormula;

        return other;
    }

    @Override
    public String getId() {
        return name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public Iterable<String> getInverseOfNames() {
        return new ConvertingIterable<Relationship, String>(inverseOfs) {
            @Override
            protected String convert(Relationship o) {
                return o.getName();
            }
        };
    }

    @Override
    public boolean getUserVisible() {
        return userVisible;
    }

    @Override
    public boolean getDeleteable() {
        return deleteable;
    }

    @Override
    public boolean getUpdateable() {
        return updateable;
    }

    @Override
    public boolean getCoreConcept() {
        return false;
    }

    public String getTitleFormula() {
        return titleFormula;
    }

    @Override
    public String getSubtitleFormula() {
        return this.subtitleFormula;
    }

    @Override
    public String getTimeFormula() {
        return this.timeFormula;
    }

    @Override
    public String[] getIntents() {
        return this.intents.toArray(new String[this.intents.size()]);
    }

    @Override
    public void addIntent(String intent, User user, Authorizations authorizations) {
        this.intents.add(intent);
    }

    @Override
    public void removeIntent(String intent, Authorizations authorizations) {
        this.intents.remove(intent);
    }

    @Override
    public String getColor() {
        return color;
    }

    @Override
    public void setProperty(String name, Value value, User user, Authorizations authorizations) {
        if (SchemaProperties.DISPLAY_NAME.getPropertyName().equals(name)) {
            this.displayName = ((TextValue) value).stringValue();
        } else if (SchemaProperties.TITLE_FORMULA.getPropertyName().equals(name)) {
            this.titleFormula = ((TextValue) value).stringValue();
        } else if (SchemaProperties.SUBTITLE_FORMULA.getPropertyName().equals(name)) {
            this.subtitleFormula = ((TextValue) value).stringValue();
        } else if (SchemaProperties.TIME_FORMULA.getPropertyName().equals(name)) {
            this.timeFormula = ((TextValue) value).stringValue();
        } else if (SchemaProperties.USER_VISIBLE.getPropertyName().equals(name)) {
            this.userVisible = ((BooleanValue) value).booleanValue();
        } else if (SchemaProperties.DELETEABLE.getPropertyName().equals(name)) {
            this.deleteable = ((BooleanValue) value).booleanValue();
        } else if (SchemaProperties.UPDATEABLE.getPropertyName().equals(name)) {
            this.updateable = ((BooleanValue) value).booleanValue();
        } else if (value != null) {
            metadata.put(name, value.toString());
        }
    }

    @Override
    public void removeProperty(String name, Authorizations authorizations) {
        if (SchemaProperties.DISPLAY_NAME.getPropertyName().equals(name)) {
            this.displayName = null;
        } else if (SchemaProperties.TITLE_FORMULA.getPropertyName().equals(name)) {
            this.titleFormula = null;
        } else if (SchemaProperties.SUBTITLE_FORMULA.getPropertyName().equals(name)) {
            this.subtitleFormula = null;
        } else if (SchemaProperties.TIME_FORMULA.getPropertyName().equals(name)) {
            this.timeFormula = null;
        } else if (SchemaProperties.USER_VISIBLE.getPropertyName().equals(name)) {
            this.userVisible = false;
        } else if (SchemaProperties.DELETEABLE.getPropertyName().equals(name)) {
            this.deleteable = false;
        } else if (SchemaProperties.UPDATEABLE.getPropertyName().equals(name)) {
            this.updateable = false;
        } else if (SchemaProperties.INTENT.getPropertyName().equals(name)) {
            intents.clear();
        } else {
            metadata.remove(name);
        }
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void addInverseOf(Relationship inverseOfRelationship) {
        inverseOfs.add(inverseOfRelationship);
    }

    public String getWorkspaceId() {
        return workspaceId;
    }

    public void removeWorkspaceId() {
        workspaceId = null;
    }

    void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    @Override
    public SandboxStatus getSandboxStatus() {
        return workspaceId == null ? SandboxStatus.PUBLIC : SandboxStatus.PRIVATE;
    }
}
