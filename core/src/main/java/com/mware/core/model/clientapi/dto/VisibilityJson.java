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
package com.mware.core.model.clientapi.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.mware.core.model.clientapi.util.ClientApiConverter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VisibilityJson {
    private String source = "";
    private Set<String> workspaces = new HashSet<String>();

    public VisibilityJson() {

    }

    public VisibilityJson(String source) {
        if (source == null) {
            source = "";
        }
        this.source = source;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Set<String> getWorkspaces() {
        return workspaces;
    }

    public void addWorkspace(String workspaceId) {
        if (workspaceId != null) {
            workspaces.add(workspaceId);
        }
    }

    @Override
    public String toString() {
        return ClientApiConverter.clientApiToString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VisibilityJson that = (VisibilityJson) o;
        if (source != null ? !source.equals(that.source) : that.source != null) return false;
        if (workspaces != null ? !workspaces.equals(that.workspaces) : that.workspaces != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return source.hashCode();
    }

    public static VisibilityJson removeFromWorkspace(VisibilityJson json, String workspaceId) {
        if (json == null) {
            json = new VisibilityJson();
        }

        json.getWorkspaces().remove(workspaceId);
        return json;
    }

    public static VisibilityJson removeFromAllWorkspace(VisibilityJson json) {
        if (json == null) {
            json = new VisibilityJson();
        }

        json.getWorkspaces().clear();
        return json;
    }

    public static VisibilityJson updateVisibilitySourceAndAddWorkspaceId(VisibilityJson visibilityJson,
                                                                         String visibilitySource, String workspaceId) {
        if (visibilityJson == null) {
            visibilityJson = new VisibilityJson();
        }

        visibilityJson.setSource(visibilitySource);
        visibilityJson.addWorkspace(workspaceId);

        return visibilityJson;
    }

    public static VisibilityJson updateVisibilitySource(VisibilityJson visibilityJson, String visibilitySource) {
        if (visibilityJson == null) {
            visibilityJson = new VisibilityJson();
        }

        visibilityJson.setSource(visibilitySource);
        return visibilityJson;
    }

    public static boolean isVisibilityJson(Map map) {
        return map.size() == 2 && map.containsKey("source") && map.containsKey("workspaces");
    }

    public static VisibilityJson fromMap(Map map) {
        VisibilityJson visibilityJson = new VisibilityJson();
        visibilityJson.setSource((String) map.get("source"));
        List<String> workspaces = (List<String>) map.get("workspaces");
        for (String workspace : workspaces) {
            visibilityJson.addWorkspace(workspace);
        }
        return visibilityJson;
    }
}
