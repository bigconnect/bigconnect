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
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.user.User;
import com.mware.core.util.JSONUtil;
import com.mware.core.util.SandboxStatusUtil;
import com.mware.ge.*;
import com.mware.ge.util.CloseableUtils;
import com.mware.ge.util.IterableUtils;
import com.mware.ge.values.storable.Value;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GeSchemaProperty extends SchemaProperty {
    private final Vertex vertex;
    private final String name;
    private ImmutableList<String> dependentPropertyNames;
    private String workspaceId;

    public GeSchemaProperty(Vertex vertex, ImmutableList<String> dependentPropertyNames, String workspaceId) {
        this.vertex = vertex;
        this.dependentPropertyNames = dependentPropertyNames;
        this.name = SchemaProperties.ONTOLOGY_TITLE.getPropertyValue(vertex);
        this.workspaceId = workspaceId;
    }

    @Override
    public void setProperty(String name, Value value, User user, Authorizations authorizations) {
        Visibility visibility = SchemaRepository.VISIBILITY.getVisibility();

        Metadata metadata = Metadata.create();
        BcSchema.MODIFIED_DATE_METADATA.setMetadata(metadata, ZonedDateTime.now(), visibility);
        if (user != null) {
            BcSchema.MODIFIED_BY_METADATA.setMetadata(metadata, user.getUserId(), visibility);
        }

        getVertex().setProperty(name, value, metadata, visibility, authorizations);
        getVertex().getGraph().flush();
    }

    @Override
    public String getId() {
        return vertex.getId();
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return SchemaProperties.DISPLAY_NAME.getPropertyValue(vertex);
    }

    public String getPropertyGroup() {
        return SchemaProperties.PROPERTY_GROUP.getPropertyValue(vertex);
    }

    @Override
    public String getValidationFormula() {
        return SchemaProperties.VALIDATION_FORMULA.getPropertyValue(vertex);
    }

    @Override
    public String getDisplayFormula() {
        return SchemaProperties.DISPLAY_FORMULA.getPropertyValue(vertex);
    }

    @Override
    public ImmutableList<String> getDependentPropertyNames() {
        return this.dependentPropertyNames;
    }

    public String[] getIntents() {
        return IterableUtils.toArray(SchemaProperties.INTENT.getPropertyValues(vertex), String.class);
    }

    @Override
    public TextIndexHint[] getTextIndexHints() {
        Iterable<String> strTextIndexHints = SchemaProperties.TEXT_INDEX_HINTS.getPropertyValues(vertex);
        Set<TextIndexHint> textIndexHints = new HashSet<>();
        for (String strTextHint : strTextIndexHints) {
            try {
                textIndexHints.add(TextIndexHint.valueOf(strTextHint));
            } catch (Throwable t) {
                // ignore
            }
        }
        return IterableUtils.toArray(textIndexHints, TextIndexHint.class);
    }

    @Override
    public void addTextIndexHint(TextIndexHint textIndexHint, Authorizations authorizations) {
        SchemaProperties.TEXT_INDEX_HINTS.addPropertyValue(vertex, textIndexHint.name(), textIndexHint.name(), SchemaRepository.VISIBILITY.getVisibility(), authorizations);
        getVertex().getGraph().flush();
    }

    @Override
    public void removeTextIndexHint(TextIndexHint textIndexHint, Authorizations authorizations) {
        SchemaProperties.TEXT_INDEX_HINTS.removeProperty(vertex, textIndexHint.name(), authorizations);
        SchemaProperties.TEXT_INDEX_HINTS.removeProperty(vertex, "", authorizations);
        getVertex().getGraph().flush();
    }

    @Override
    public void addIntent(String intent, Authorizations authorizations) {
        SchemaProperties.INTENT.addPropertyValue(vertex, intent, intent, SchemaRepository.VISIBILITY.getVisibility(), authorizations);
        getVertex().getGraph().flush();
    }

    @Override
    public void removeIntent(String intent, Authorizations authorizations) {
        SchemaProperties.INTENT.removeProperty(vertex, intent, authorizations);
        getVertex().getGraph().flush();
    }

    public boolean getUserVisible() {
        Boolean b = SchemaProperties.USER_VISIBLE.getPropertyValue(vertex);
        if (b == null) {
            return true;
        }
        return b;
    }

    @Override
    public boolean getSearchFacet() {
        Boolean b = SchemaProperties.SEARCH_FACET.getPropertyValue(vertex);
        if (b == null) {
            return false;
        }
        return b;
    }

    @Override
    public boolean getSystemProperty() {
        Boolean b = SchemaProperties.SYSTEM_PROPERTY.getPropertyValue(vertex);
        if (b == null) {
            return false;
        }
        return b;
    }

    @Override
    public String getAggType() {
        return SchemaProperties.AGG_TYPE.getPropertyValue(vertex);
    }

    @Override
    public int getAggPrecision() {
        Integer l = SchemaProperties.AGG_PRECISION.getPropertyValue(vertex);
        if (l != null)
            return l;
        else
            return 0;
    }

    @Override
    public String getAggInterval() {
        return SchemaProperties.AGG_INTERVAL.getPropertyValue(vertex);
    }

    @Override
    public long getAggMinDocumentCount() {
        Long l = SchemaProperties.AGG_MIN_DOCUMENT_COUNT.getPropertyValue(vertex);
        if (l != null)
            return l;
        else
            return 0;
    }

    @Override
    public String getAggTimeZone() {
        return SchemaProperties.AGG_TIMEZONE.getPropertyValue(vertex);
    }

    @Override
    public String getAggCalendarField() {
        return SchemaProperties.AGG_CALENDAR_FIELD.getPropertyValue(vertex);
    }

    public boolean getSearchable() {
        Boolean b = SchemaProperties.SEARCHABLE.getPropertyValue(vertex);
        if (b == null) {
            return true;
        }
        return b;
    }

    public boolean getSortable() {
        Boolean b = SchemaProperties.SORTABLE.getPropertyValue(vertex);
        if (b == null) {
            return true;
        }
        return b;
    }

    @Override
    public Integer getSortPriority() {
        return SchemaProperties.SORT_PRIORITY.getPropertyValue(vertex);
    }

    public boolean getUpdateable() {
        Boolean b = SchemaProperties.UPDATEABLE.getPropertyValue(vertex);
        if (b == null) {
            return true;
        }
        return b;
    }

    public boolean getDeleteable() {
        Boolean b = SchemaProperties.DELETEABLE.getPropertyValue(vertex);
        if (b == null) {
            return true;
        }
        return b;
    }

    @Override
    public boolean getAddable() {
        Boolean b = SchemaProperties.ADDABLE.getPropertyValue(vertex);
        if (b == null) {
            return true;
        }
        return b;
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

    public PropertyType getDataType() {
        return PropertyType.convert(SchemaProperties.DATA_TYPE.getPropertyValue(vertex));
    }

    public static PropertyType getDataType(Vertex vertex) {
        return PropertyType.convert(SchemaProperties.DATA_TYPE.getPropertyValue(vertex));
    }

    public String getDisplayType() {
        return SchemaProperties.DISPLAY_TYPE.getPropertyValue(vertex);
    }

    @Override
    public Double getBoost() {
        return SchemaProperties.BOOST.getPropertyValue(vertex);
    }

    public Map<String, String> getPossibleValues() {
        JSONObject propertyValue = SchemaProperties.POSSIBLE_VALUES.getPropertyValue(vertex);
        if (propertyValue == null) {
            return null;
        }
        return JSONUtil.toStringMap(propertyValue);
    }

    public Vertex getVertex() {
        return this.vertex;
    }

    public void setDependentProperties(Collection<String> newDependentPropertyNames) {
        this.dependentPropertyNames = ImmutableList.copyOf(newDependentPropertyNames);
    }

    @Override
    public SandboxStatus getSandboxStatus() {
        return SandboxStatusUtil.getSandboxStatus(this.vertex, this.workspaceId);
    }

    @Override
    public List<String> getConceptNames() {
        return getAssociatedElements(SchemaRepository.TYPE_CONCEPT);
    }

    @Override
    public List<String> getRelationshipNames() {
        return getAssociatedElements(SchemaRepository.TYPE_RELATIONSHIP);
    }

    private List<String> getAssociatedElements(String elementType) {
        Iterable<Vertex> vertices = vertex.getVertices(Direction.BOTH, LabelName.HAS_PROPERTY.toString(), vertex.getAuthorizations());
        List<String> result = StreamSupport.stream(vertices.spliterator(), false)
                .filter(v -> elementType.equals(v.getConceptType()))
                .map(SchemaProperties.ONTOLOGY_TITLE::getPropertyValue)
                .collect(Collectors.toList());
        CloseableUtils.closeQuietly(vertices);
        return result;
    }
}
