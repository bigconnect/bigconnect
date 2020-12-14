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
package com.mware.core.util;

import com.mware.core.model.clientapi.dto.SandboxStatus;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.properties.BcSchema;
import com.mware.ge.Element;
import com.mware.ge.Property;

import java.util.Collection;
import java.util.List;

public class SandboxStatusUtil {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SandboxStatusUtil.class);

    public static SandboxStatus getSandboxStatus(Element element, String workspaceId) {
        VisibilityJson visibilityJson = BcSchema.VISIBILITY_JSON.getPropertyValue(element);
        return SandboxStatus.getFromVisibilityJsonString(visibilityJson, workspaceId);
    }

    public static SandboxStatus[] getPropertySandboxStatuses(List<Property> properties, String workspaceId) {
        SandboxStatus[] sandboxStatuses = new SandboxStatus[properties.size()];
        for (int i = 0; i < properties.size(); i++) {
            Property property = properties.get(i);
            Collection<VisibilityJson> visibilityJsons = BcSchema.VISIBILITY_JSON_METADATA.getMetadataValues(property.getMetadata());
            if (visibilityJsons.size() > 1) {
                //LOGGER.error("Multiple %s found on property %s. Choosing the best match.", BcProperties.VISIBILITY_JSON_METADATA.getMetadataKey(), property);
            }
            sandboxStatuses[i] = getMostExclusiveSandboxStatus(visibilityJsons, workspaceId);
        }

        for (int i = 0; i < properties.size(); i++) {
            Property property = properties.get(i);
            if (sandboxStatuses[i] != SandboxStatus.PRIVATE) {
                continue;
            }
            for (int j = 0; j < properties.size(); j++) {
                Property p = properties.get(j);
                if (i == j) {
                    continue;
                }

                if (sandboxStatuses[j] == SandboxStatus.PUBLIC &&
                        sandboxStatuses[i] == SandboxStatus.PRIVATE &&
                        property.getKey().equals(p.getKey()) &&
                        property.getName().equals(p.getName())) {
                    sandboxStatuses[i] = SandboxStatus.PUBLIC_CHANGED;
                }
            }
        }

        return sandboxStatuses;
    }

    private static SandboxStatus getMostExclusiveSandboxStatus(Collection<VisibilityJson> visibilityJsons, String workspaceId) {
        for (VisibilityJson visibilityJson : visibilityJsons) {
            SandboxStatus status = SandboxStatus.getFromVisibilityJsonString(visibilityJson, workspaceId);
            switch (status) {
                case PUBLIC:
                    break;
                case PUBLIC_CHANGED:
                case PRIVATE:
                    return status;
            }
        }
        return SandboxStatus.PUBLIC;
    }
}
