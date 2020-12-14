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

import com.mware.core.exception.BcResourceNotFoundException;
import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.user.User;
import com.mware.core.util.JSONUtil;
import com.mware.ge.Authorizations;
import com.mware.ge.values.storable.*;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.*;

public class InMemoryConcept extends Concept {
    private String name;
    private String color;
    private String displayName;
    private String displayType;
    private String titleFormula;
    private String subtitleFormula;
    private String timeFormula;
    private List<String> addRelatedConceptWhiteList;
    private byte[] glyphIcon;
    private String glyphIconFilePath;
    private byte[] glyphIconSelected;
    private String glyphIconSelectedFilePath;
    private byte[] mapGlyphIcon;
    private boolean userVisible = true;
    private boolean updateable = true;
    private boolean deleteable = true;
    private boolean coreConcept = true;
    private Boolean searchable;
    private Boolean addable;
    private Map<String, String> metadata = new HashMap<>();
    private Set<String> intents = new HashSet<>();
    private String workspaceId;

    public InMemoryConcept(String conceptName, String parentName, String workspaceId) {
        super(parentName, new ArrayList<>());
        this.name = conceptName;
        this.workspaceId = workspaceId;
    }

    InMemoryConcept shallowCopy() {
        InMemoryConcept other = new InMemoryConcept(name, getParentConceptName(), workspaceId);
        other.getProperties().addAll(getProperties());
        other.name = name;
        other.color = color;
        other.displayName = displayName;
        other.displayType = displayType;
        other.titleFormula = titleFormula;
        other.subtitleFormula = subtitleFormula;
        other.timeFormula = timeFormula;
        other.addRelatedConceptWhiteList = addRelatedConceptWhiteList;
        other.glyphIcon = glyphIcon;
        other.glyphIconFilePath = glyphIconFilePath;
        other.glyphIconSelected = glyphIconSelected;
        other.glyphIconSelectedFilePath = glyphIconSelectedFilePath;
        other.mapGlyphIcon = mapGlyphIcon;
        other.userVisible = userVisible;
        other.updateable = updateable;
        other.deleteable = deleteable;
        other.searchable = searchable;
        other.addable = addable;
        other.metadata.putAll(metadata);
        other.intents.addAll(intents);
        return other;
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
    public String getId() {
        return name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean hasGlyphIconResource() {
        return glyphIcon != null || glyphIconFilePath != null;
    }

    @Override
    public boolean hasGlyphIconSelectedResource() {
        return glyphIconSelected != null || glyphIconSelectedFilePath != null;
    }

    @Override
    public String getColor() {
        return color;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDisplayType() {
        return displayType;
    }

    @Override
    public String getTitleFormula() {
        return titleFormula;
    }

    @Override
    public Boolean getSearchable() {
        return searchable;
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

    @Override
    public Map<String, String> getMetadata() {
        return this.metadata;
    }

    @Override
    public List<String> getAddRelatedConceptWhiteList() {
        return addRelatedConceptWhiteList;
    }

    @Override
    public void setProperty(String name, Value value, User user, Authorizations authorizations) {
        if (SchemaProperties.COLOR.getPropertyName().equals(name)) {
            this.color = ((TextValue) value).stringValue();
        } else if (SchemaProperties.DISPLAY_TYPE.getPropertyName().equals(name)) {
            this.displayType = ((TextValue) value).stringValue();
        } else if (SchemaProperties.TITLE_FORMULA.getPropertyName().equals(name)) {
            this.titleFormula = ((TextValue) value).stringValue();
        } else if (SchemaProperties.SUBTITLE_FORMULA.getPropertyName().equals(name)) {
            this.subtitleFormula = ((TextValue) value).stringValue();
        } else if (SchemaProperties.TIME_FORMULA.getPropertyName().equals(name)) {
            this.timeFormula = ((TextValue) value).stringValue();
        } else if (SchemaProperties.USER_VISIBLE.getPropertyName().equals(name)) {
            this.userVisible = ((BooleanValue) value).booleanValue();
        } else if (SchemaProperties.GLYPH_ICON.getPropertyName().equals(name)) {
            try {
                StreamingPropertyValue spv = (StreamingPropertyValue) value;
                if(spv != null) {
                    this.glyphIcon = IOUtils.toByteArray(spv.getInputStream());
                }
            } catch (IOException e) {
                throw new BcResourceNotFoundException("Could not retrieve glyph icon");
            }
        } else if (SchemaProperties.GLYPH_ICON_FILE_NAME.getPropertyName().equals(name)) {
            this.glyphIconFilePath = ((TextValue) value).stringValue();
        } else if (SchemaProperties.GLYPH_ICON_SELECTED.getPropertyName().equals(name)) {
            this.glyphIconSelected = ((ByteArray) value).asObjectCopy();
        } else if (SchemaProperties.GLYPH_ICON_SELECTED_FILE_NAME.getPropertyName().equals(name)) {
            this.glyphIconSelectedFilePath = ((TextValue) value).stringValue();
        } else if (SchemaProperties.MAP_GLYPH_ICON.getPropertyName().equals(name)) {
            this.mapGlyphIcon = ((ByteArray) value).asObjectCopy();
        } else if (SchemaProperties.TITLE.getPropertyName().equals(name)) {
            this.name = ((TextValue) value).stringValue();
        } else if (SchemaProperties.DISPLAY_NAME.getPropertyName().equals(name)) {
            this.displayName = ((TextValue) value).stringValue();
        } else if (SchemaProperties.ADD_RELATED_CONCEPT_WHITE_LIST.getPropertyName().equals(name)) {
            this.addRelatedConceptWhiteList = JSONUtil.toStringList(JSONUtil.parseArray(((TextValue) value).stringValue()));
        } else if (SchemaProperties.SEARCHABLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.searchable = ((BooleanValue) value).booleanValue();
            } else {
                this.searchable = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (SchemaProperties.ADDABLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.addable = ((BooleanValue) value).booleanValue();
            } else {
                this.addable = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (SchemaProperties.UPDATEABLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.updateable = ((BooleanValue) value).booleanValue();
            } else {
                this.updateable = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (SchemaProperties.DELETEABLE.getPropertyName().equals(name)) {
            if (value instanceof BooleanValue) {
                this.deleteable = ((BooleanValue) value).booleanValue();
            } else {
                this.deleteable = Boolean.parseBoolean(((TextValue) value).stringValue());
            }
        } else if (value != null) {
            metadata.put(name, value.toString());
        }
    }

    @Override
    public void removeProperty(String name, Authorizations authorizations) {
        if (SchemaProperties.COLOR.getPropertyName().equals(name)) {
            this.color = null;
        } else if (SchemaProperties.DISPLAY_TYPE.getPropertyName().equals(name)) {
            this.displayType = null;
        } else if (SchemaProperties.TITLE_FORMULA.getPropertyName().equals(name)) {
            this.titleFormula = null;
        } else if (SchemaProperties.SUBTITLE_FORMULA.getPropertyName().equals(name)) {
            this.subtitleFormula = null;
        } else if (SchemaProperties.TIME_FORMULA.getPropertyName().equals(name)) {
            this.timeFormula = null;
        } else if (SchemaProperties.USER_VISIBLE.getPropertyName().equals(name)) {
            this.userVisible = true;
        } else if (SchemaProperties.GLYPH_ICON.getPropertyName().equals(name)) {
            this.glyphIcon = null;
        } else if (SchemaProperties.GLYPH_ICON_FILE_NAME.getPropertyName().equals(name)) {
            this.glyphIconFilePath = null;
        } else if (SchemaProperties.GLYPH_ICON_SELECTED.getPropertyName().equals(name)) {
            this.glyphIconSelected = null;
        } else if (SchemaProperties.GLYPH_ICON_SELECTED_FILE_NAME.getPropertyName().equals(name)) {
            this.glyphIconSelectedFilePath = null;
        } else if (SchemaProperties.MAP_GLYPH_ICON.getPropertyName().equals(name)) {
            this.mapGlyphIcon = null;
        } else if (SchemaProperties.TITLE.getPropertyName().equals(name)) {
            this.name = null;
        } else if (SchemaProperties.DISPLAY_NAME.getPropertyName().equals(name)) {
            this.displayName = null;
        } else if (SchemaProperties.ADD_RELATED_CONCEPT_WHITE_LIST.getPropertyName().equals(name)) {
            this.addRelatedConceptWhiteList = Collections.emptyList();
        } else if (SchemaProperties.SEARCHABLE.getPropertyName().equals(name)) {
            this.searchable = null;
        } else if (SchemaProperties.ADDABLE.getPropertyName().equals(name)) {
            this.addable = null;
        } else if (SchemaProperties.UPDATEABLE.getPropertyName().equals(name)) {
            this.updateable = true;
        } else if (SchemaProperties.DELETEABLE.getPropertyName().equals(name)) {
            this.deleteable = true;
        } else if (SchemaProperties.INTENT.getPropertyName().equals(name)) {
            intents.clear();
        } else if (metadata.containsKey(name)) {
            metadata.remove(name);
        }
    }

    @Override
    public byte[] getGlyphIcon() {
        return glyphIcon;
    }

    @Override
    public byte[] getGlyphIconSelected() {
        return glyphIconSelected;
    }

    @Override
    public String getGlyphIconFilePath() {
        return glyphIconFilePath;
    }

    @Override
    public String getGlyphIconSelectedFilePath() {
        return glyphIconSelectedFilePath;
    }

    @Override
    public byte[] getMapGlyphIcon() {
        return mapGlyphIcon;
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

