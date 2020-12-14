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

import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.user.User;
import com.mware.core.util.SandboxStatusUtil;
import com.mware.ge.Authorizations;
import com.mware.ge.Metadata;
import com.mware.ge.Vertex;
import com.mware.ge.Visibility;
import com.mware.ge.mutation.ElementMutation;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.Value;

import java.time.ZonedDateTime;
import java.util.*;

public class GeRelationship extends Relationship {
    private final Vertex vertex;
    private final List<String> inverseOfNames;
    private final String namespace;

    public GeRelationship(
            String parentName,
            Vertex vertex,
            List<String> domainConceptNames,
            List<String> rangeConceptNames,
            List<String> inverseOfNames,
            Collection<SchemaProperty> properties,
            String namespace
    ) {
        super(parentName, domainConceptNames, rangeConceptNames, properties);
        this.vertex = vertex;
        this.inverseOfNames = inverseOfNames;
        this.namespace = namespace;
    }

    @Override
    public String getId() {
        return vertex.getId();
    }

    @Override
    public String[] getIntents() {
        return IterableUtils.toArray(SchemaProperties.INTENT.getPropertyValues(vertex), String.class);
    }

    @Override
    public void addIntent(String intent, User user, Authorizations authorizations) {
        Visibility visibility = SchemaRepository.VISIBILITY.getVisibility();
        Metadata metadata = createPropertyMetadata(user, ZonedDateTime.now(), visibility);
        SchemaProperties.INTENT.addPropertyValue(vertex, intent, intent, metadata, visibility, authorizations);
        getVertex().getGraph().flush();
    }

    @Override
    public void removeIntent(String intent, Authorizations authorizations) {
        SchemaProperties.INTENT.removeProperty(vertex, intent, authorizations);
        getVertex().getGraph().flush();
    }

    @Override
    public void setProperty(String name, Value value, User user, Authorizations authorizations) {
        Visibility visibility = SchemaRepository.VISIBILITY.getVisibility();
        Metadata metadata = createPropertyMetadata(user, ZonedDateTime.now(), visibility);
        getVertex().setProperty(name, value, metadata, visibility, authorizations);
        getVertex().getGraph().flush();
    }

    @Override
    public void removeProperty(String name, Authorizations authorizations) {
        getVertex().softDeleteProperty(ElementMutation.DEFAULT_KEY, name, authorizations);
        getVertex().getGraph().flush();
    }

    @Override
    public String getColor() {
        return SchemaProperties.COLOR.getPropertyValue(vertex);
    }

    @Override
    public String getTitleFormula() {
        return SchemaProperties.TITLE_FORMULA.getPropertyValue(vertex);
    }

    @Override
    public String getSubtitleFormula() {
        return SchemaProperties.SUBTITLE_FORMULA.getPropertyValue(vertex);
    }

    @Override
    public String getTimeFormula() {
        return SchemaProperties.TIME_FORMULA.getPropertyValue(vertex);
    }

    public String getName() {
        return SchemaProperties.ONTOLOGY_TITLE.getPropertyValue(vertex);
    }

    public String getDisplayName() {
        return SchemaProperties.DISPLAY_NAME.getPropertyValue(vertex);
    }

    @Override
    public Iterable<String> getInverseOfNames() {
        return inverseOfNames;
    }

    @Override
    public boolean getUserVisible() {
        return SchemaProperties.USER_VISIBLE.getPropertyValue(vertex, true);
    }

    @Override
    public boolean getDeleteable() {
        return SchemaProperties.DELETEABLE.getPropertyValue(vertex, true);
    }

    @Override
    public boolean getUpdateable() {
        return SchemaProperties.UPDATEABLE.getPropertyValue(vertex, true);
    }

    @Override
    public boolean getCoreConcept() {
        return SchemaProperties.CORE_CONCEPT.getPropertyValue(vertex, false);
    }

    @Override
    public Map<String, String> getMetadata() {
        Map<String, String> metadata = new HashMap<>();
        if (getSandboxStatus() == SandboxStatus.PRIVATE) {
            if (BcSchema.MODIFIED_BY.hasProperty(vertex)) {
                metadata.put(BcSchema.MODIFIED_BY.getPropertyName(), BcSchema.MODIFIED_BY.getPropertyValue(vertex));
            }
            if (BcSchema.MODIFIED_DATE.hasProperty(vertex)) {
                metadata.put(BcSchema.MODIFIED_DATE.getPropertyName(), BcSchema.MODIFIED_DATE.getPropertyValue(vertex).toString());
            }
        }
        return metadata;
    }

    public Vertex getVertex() {
        return vertex;
    }

    @Override
    public SandboxStatus getSandboxStatus() {
        return SandboxStatusUtil.getSandboxStatus(this.vertex, this.namespace);
    }

    private Metadata createPropertyMetadata(User user, ZonedDateTime modifiedDate, Visibility visibility) {
        Metadata metadata = Metadata.create();
        BcSchema.MODIFIED_DATE_METADATA.setMetadata(metadata, modifiedDate, visibility);
        if (user != null) {
            BcSchema.MODIFIED_BY_METADATA.setMetadata(metadata, user.getUserId(), visibility);
        }
        return metadata;
    }
}
