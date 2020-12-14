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
package com.mware.core.ingest.dataworker;

import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.User;
import com.mware.ge.*;

import java.io.File;
import java.time.ZonedDateTime;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

public class DataWorkerData {
    private final VisibilityTranslator visibilityTranslator;
    private final Element element;
    private final Property property;
    private final String workspaceId;
    private final String visibilitySource;
    private final Priority priority;
    private final boolean traceEnabled;
    private File localFile;
    private long beforeActionTimestamp;
    private ElementOrPropertyStatus status;

    public DataWorkerData(
            VisibilityTranslator visibilityTranslator,
            Element element,
            Property property,
            String workspaceId,
            String visibilitySource,
            Priority priority,
            boolean traceEnabled
    ) {
        checkNotNull(priority, "priority cannot be null");
        this.visibilityTranslator = visibilityTranslator;
        this.element = element;
        this.property = property;
        this.workspaceId = workspaceId;
        this.visibilitySource = visibilitySource;
        this.priority = priority;
        this.traceEnabled = traceEnabled;
    }

    public DataWorkerData(
            VisibilityTranslator visibilityTranslator,
            Element element,
            Property property,
            String workspaceId,
            String visibilitySource,
            Priority priority,
            boolean traceEnabled,
            long beforeActionTimestamp,
            ElementOrPropertyStatus status
    ) {
        checkNotNull(priority, "priority cannot be null");
        this.visibilityTranslator = visibilityTranslator;
        this.element = element;
        this.property = property;
        this.workspaceId = workspaceId;
        this.visibilitySource = visibilitySource;
        this.priority = priority;
        this.beforeActionTimestamp = beforeActionTimestamp;
        this.status = status;
        this.traceEnabled = traceEnabled;
    }

    public Element getElement() {
        return element;
    }

    public Property getProperty() {
        return property;
    }

    public void setLocalFile(File localFile) {
        this.localFile = localFile;
    }

    public File getLocalFile() {
        return localFile;
    }

    public Visibility getVisibility() {
        return getElement().getVisibility();
    }

    public String getWorkspaceId() {
        return workspaceId;
    }

    public String getVisibilitySource() {
        return visibilitySource;
    }

    public Priority getPriority() {
        return priority;
    }

    public long getBeforeActionTimestamp() {
        return beforeActionTimestamp;
    }

    public ElementOrPropertyStatus getPropertyStatus() {
        return status;
    }

    public boolean isTraceEnabled() {
        return traceEnabled;
    }

    // TODO this is a weird method. I'm not sure what this should be used for
    public VisibilityJson getVisibilitySourceJson() {
        if (getVisibilitySource() == null || getVisibilitySource().length() == 0) {
            return new VisibilityJson();
        }
        VisibilityJson visibilityJson = new VisibilityJson();
        visibilityJson.setSource(getVisibilitySource());
        return visibilityJson;
    }

    /**
     * @deprecated  replaced by {@link #getElementVisibilityJson()}
     */
    @Deprecated
    public VisibilityJson getVisibilityJson() {
        return getElementVisibilityJson();
    }


    public VisibilityJson getElementVisibilityJson() {
        VisibilityJson visibilityJson = BcSchema.VISIBILITY_JSON.getPropertyValue(getElement());
        if (visibilityJson != null) {
            return visibilityJson;
        }

        return getVisibilitySourceJson();
    }

    public VisibilityJson getPropertyVisibilityJson() {
        if (property != null) {
            VisibilityJson propertyVisibilityJson = BcSchema.VISIBILITY_JSON_METADATA.getMetadataValue(property);
            if (propertyVisibilityJson != null) {
                return propertyVisibilityJson;
            }
        }
        return getElementVisibilityJson();
    }

    public Metadata createPropertyMetadata(User user) {
        Metadata metadata = Metadata.create();
        VisibilityJson visibilityJson = getPropertyVisibilityJson();
        Visibility defaultVisibility = visibilityTranslator.getDefaultVisibility();
        if (visibilityJson != null) {
            BcSchema.VISIBILITY_JSON_METADATA.setMetadata(metadata, visibilityJson, defaultVisibility);
        }
        BcSchema.MODIFIED_DATE_METADATA.setMetadata(metadata, ZonedDateTime.now(), defaultVisibility);
        BcSchema.MODIFIED_BY_METADATA.setMetadata(metadata, user.getUserId(), defaultVisibility);
        return metadata;
    }

    public void setVisibilityJsonOnElement(ElementBuilder builder) {
        VisibilityJson visibilityJson = getElementVisibilityJson();
        if (visibilityJson != null) {
            BcSchema.VISIBILITY_JSON.setProperty(builder, visibilityJson, visibilityTranslator.getDefaultVisibility());
        }
    }

    public void setVisibilityJsonOnElement(Element element, Authorizations authorizations) {
        VisibilityJson visibilityJson = getVisibilitySourceJson();
        if (visibilityJson != null) {
            BcSchema.VISIBILITY_JSON.setProperty(element, visibilityJson, visibilityTranslator.getDefaultVisibility(), authorizations);
        }
    }

    @Override
    public String toString() {
        return "DataWorkerData{" +
                "element=" + element +
                ", property=" + property +
                ", workspaceId='" + workspaceId + '\'' +
                ", priority=" + priority +
                ", traceEnabled=" + traceEnabled +
                ", status=" + status +
                '}';
    }
}
