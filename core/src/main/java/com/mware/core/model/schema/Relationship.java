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

import com.google.common.collect.Lists;
import com.mware.core.user.User;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.ge.values.storable.Value;
import org.json.JSONException;
import com.mware.ge.Authorizations;
import com.mware.core.model.clientapi.dto.ClientApiSchema;

import java.util.*;

public abstract class Relationship implements SchemaElement, HasSchemaProperties {
    private final String parentName;
    private final List<String> sourceConceptNames;
    private final List<String> targetConceptNames;
    private final Collection<SchemaProperty> properties;

    protected Relationship(
            String parentName,
            List<String> sourceConceptNames,
            List<String> targetConceptNames,
            Collection<SchemaProperty> properties
    ) {
        this.parentName = parentName;
        this.sourceConceptNames = sourceConceptNames;
        this.targetConceptNames = targetConceptNames;
        this.properties = properties;
    }

    public abstract String getId();

    public abstract SandboxStatus getSandboxStatus();

    public abstract String getName();

    public String getParentName() {
        return parentName;
    }

    public abstract String getTitleFormula();

    public abstract String getSubtitleFormula();

    public abstract String getTimeFormula();

    public abstract String getDisplayName();

    public abstract String getColor();

    public abstract Iterable<String> getInverseOfNames();

    public List<String> getSourceConceptNames() {
        return sourceConceptNames;
    }

    public List<String> getTargetConceptNames() {
        return targetConceptNames;
    }

    @Override
    public abstract boolean getUserVisible();

    @Override
    public abstract boolean getDeleteable();

    @Override
    public abstract boolean getUpdateable();

    public abstract String[] getIntents();

    public Collection<SchemaProperty> getProperties() {
        return properties;
    }

    public abstract void addIntent(String intent, User user, Authorizations authorizations);

    public abstract void removeIntent(String intent, Authorizations authorizations);

    public void updateIntents(String[] newIntents, User user, Authorizations authorizations) {
        ArrayList<String> toBeRemovedIntents = Lists.newArrayList(getIntents());
        for (String newIntent : newIntents) {
            if (toBeRemovedIntents.contains(newIntent)) {
                toBeRemovedIntents.remove(newIntent);
            } else {
                addIntent(newIntent, user, authorizations);
            }
        }
        for (String toBeRemovedIntent : toBeRemovedIntents) {
            removeIntent(toBeRemovedIntent, authorizations);
        }
    }

    public abstract void setProperty(String name, Value value, User user, Authorizations authorizations);

    public abstract void removeProperty(String name, Authorizations authorizations);

    public abstract Map<String, String> getMetadata();

    public ClientApiSchema.Relationship toClientApi() {
        try {
            ClientApiSchema.Relationship result = new ClientApiSchema.Relationship();
            result.setParentName(getParentName());
            result.setTitle(getName());
            result.setDisplayName(getDisplayName());
            result.setDomainConceptNames(getSourceConceptNames());
            result.setRangeConceptNames(getTargetConceptNames());
            result.setUserVisible(getUserVisible());
            result.setDeleteable(getDeleteable());
            result.setUpdateable(getUpdateable());
            result.setTitleFormula(getTitleFormula());
            result.setSubtitleFormula(getSubtitleFormula());
            result.setTimeFormula(getTimeFormula());
            result.setCoreConcept(getCoreConcept());

            if (getIntents() != null) {
                result.getIntents().addAll(Arrays.asList(getIntents()));
            }
            for (Map.Entry<String, String> additionalProperty : getMetadata().entrySet()) {
                result.getMetadata().put(additionalProperty.getKey(), additionalProperty.getValue());
            }

            result.setSandboxStatus(getSandboxStatus());

            if (getColor() != null) {
                result.setColor(getColor());
            }

            Iterable<String> inverseOfNames = getInverseOfNames();
            for (String inverseOfName : inverseOfNames) {
                ClientApiSchema.Relationship.InverseOf inverseOf = new ClientApiSchema.Relationship.InverseOf();
                inverseOf.setName(inverseOfName);
                inverseOf.setPrimaryName(getPrimaryInverseOfName(getName(), inverseOfName));
                result.getInverseOfs().add(inverseOf);
            }

            if (this.properties != null) {
                for (SchemaProperty property : this.properties) {
                    result.getProperties().add(property.getName());
                }
            }

            return result;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getPrimaryInverseOfName(String name1, String name2) {
        if (name1.compareTo(name2) > 0) {
            return name2;
        }
        return name1;
    }

    public static Collection<ClientApiSchema.Relationship> toClientApiRelationships(Iterable<Relationship> relationships) {
        Collection<ClientApiSchema.Relationship> results = new ArrayList<>();
        for (Relationship vertex : relationships) {
            results.add(vertex.toClientApi());
        }
        return results;
    }

    @Override
    public String toString() {
        return "Relationship{" +
                "name='" + getName() + '\'' +
                '}';
    }
}
