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
import org.atteo.evo.inflector.English;
import com.mware.ge.Authorizations;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.ClientApiSchema;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

public abstract class Concept implements SchemaElement, HasSchemaProperties {
    private final String parentConceptName;
    private final Collection<SchemaProperty> properties;

    protected Concept(String parentConceptName, Collection<SchemaProperty> properties) {
        this.parentConceptName = parentConceptName;
        this.properties = properties;
    }

    public abstract String getId();

    public abstract String getName();

    public abstract boolean hasGlyphIconResource();

    public abstract boolean hasGlyphIconSelectedResource();

    public abstract String getColor();

    public abstract String getDisplayName();

    public abstract String getDisplayType();

    public abstract String getTitleFormula();

    public abstract Boolean getSearchable();

    public abstract String getSubtitleFormula();

    public abstract String getTimeFormula();

    @Override
    public abstract boolean getUserVisible();

    @Override
    public abstract boolean getDeleteable();

    @Override
    public abstract boolean getUpdateable();

    public abstract SandboxStatus getSandboxStatus();

    public abstract Map<String, String> getMetadata();

    public abstract List<String> getAddRelatedConceptWhiteList();

    public Collection<SchemaProperty> getProperties() {
        return properties;
    }

    public String getParentConceptName() {
        return this.parentConceptName;
    }

    public ClientApiSchema.Concept toClientApi() {
        try {
            ClientApiSchema.Concept concept = new ClientApiSchema.Concept();
            concept.setId(getName());
            concept.setTitle(getName());
            concept.setDisplayName(getDisplayName());
            if (getDisplayType() != null) {
                concept.setDisplayType(getDisplayType());
            }
            if (getTitleFormula() != null) {
                concept.setTitleFormula(getTitleFormula());
            }
            if (getSearchable() != null) {
                concept.setSearchable(getSearchable());
            }
            if (getSubtitleFormula() != null) {
                concept.setSubtitleFormula(getSubtitleFormula());
            }
            if (getTimeFormula() != null) {
                concept.setTimeFormula(getTimeFormula());
            }
            if (getParentConceptName() != null) {
                concept.setParentConcept(getParentConceptName());
            }
            if (getDisplayName() != null) {
                concept.setPluralDisplayName(English.plural(getDisplayName()));
            }

            concept.setUserVisible(getUserVisible());
            concept.setCoreConcept(getCoreConcept());
            concept.setDeleteable(getDeleteable());
            concept.setUpdateable(getUpdateable());

            if (hasGlyphIconResource()) {
                concept.setGlyphIconHref("resource?id=" + URLEncoder.encode(getName(), "utf8"));
            }
            if (hasGlyphIconSelectedResource()) {
                concept.setGlyphIconSelectedHref("resource?state=selected&id=" + URLEncoder.encode(getName(), "utf8"));
            }
            if (getColor() != null) {
                concept.setColor(getColor());
            }
            if (getAddRelatedConceptWhiteList() != null) {
                concept.getAddRelatedConceptWhiteList().addAll(getAddRelatedConceptWhiteList());
            }
            if (getIntents() != null) {
                concept.getIntents().addAll(Arrays.asList(getIntents()));
            }
            if (this.properties != null) {
                for (SchemaProperty property : this.properties) {
                    concept.getProperties().add(property.getName());
                }
            }
            for (Map.Entry<String, String> additionalProperty : getMetadata().entrySet()) {
                concept.getMetadata().put(additionalProperty.getKey(), additionalProperty.getValue());
            }
            if (this.getSandboxStatus() != null) {
                concept.setSandboxStatus(this.getSandboxStatus());
            }
            return concept;
        } catch (UnsupportedEncodingException e) {
            throw new BcException("bad encoding", e);
        }
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", getDisplayName(), getName());
    }

    public static Collection<ClientApiSchema.Concept> toClientApiConcepts(Iterable<Concept> concepts) {
        Collection<ClientApiSchema.Concept> results = new ArrayList<>();
        for (Concept concept : concepts) {
            results.add(concept.toClientApi());
        }
        return results;
    }

    public abstract void setProperty(String name, Value value, User user, Authorizations authorizations);

    public abstract void removeProperty(String name, Authorizations authorizations);

    public abstract byte[] getGlyphIcon();

    public abstract byte[] getGlyphIconSelected();

    public abstract String getGlyphIconFilePath();

    public abstract String getGlyphIconSelectedFilePath();

    public abstract byte[] getMapGlyphIcon();

    public abstract String[] getIntents();

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

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Concept)) {
            return false;
        }
        Concept other = (Concept) obj;
        return getName().equals(other.getName());
    }
}
