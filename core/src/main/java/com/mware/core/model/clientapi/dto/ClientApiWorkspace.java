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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ClientApiWorkspace implements ClientApiObject {
    private String workspaceId;
    private String title;
    private Boolean staging;
    private String createdBy;
    private boolean isSharedToUser;
    private boolean isEditable;
    private boolean isCommentable;
    private List<User> users = new ArrayList<User>();
    private Collection<ClientApiProduct> products;
    private Boolean active;

    public String getWorkspaceId() {
        return workspaceId;
    }

    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Boolean getStaging() {
        return staging;
    }

    public void setStaging(Boolean staging) {
        this.staging = staging;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public boolean isSharedToUser() {
        return isSharedToUser;
    }

    public void setSharedToUser(boolean isSharedToUser) {
        this.isSharedToUser = isSharedToUser;
    }

    public boolean isEditable() {
        return isEditable;
    }

    public void setEditable(boolean isEditable) {
        this.isEditable = isEditable;
    }

    public boolean isCommentable() {
        return isCommentable;
    }

    public void setCommentable(boolean isCommentable) {
        this.isCommentable = isCommentable;
    }

    public List<User> getUsers() {
        return users;
    }

    public void addUser(User user) {
        this.users.add(user);
    }

    public void setProducts(Collection<ClientApiProduct> products) {
        this.products = products;
    }

    public Collection<ClientApiProduct> getProducts() {
        return products;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public Boolean isActive() {
        return active;
    }

    @Override
    public String toString() {
        return "Workspace{" +
                "workspaceId='" + workspaceId + '\'' +
                ", title='" + title + '\'' +
                ", createdBy='" + createdBy + '\'' +
                ", isSharedToUser=" + isSharedToUser +
                ", isEditable=" + isEditable +
                '}';
    }

    public static class Vertex {
        private String vertexId;
        private boolean visible;

        public String getVertexId() {
            return vertexId;
        }

        public void setVertexId(String vertexId) {
            this.vertexId = vertexId;
        }

        public boolean isVisible() {
            return visible;
        }

        public void setVisible(boolean visible) {
            this.visible = visible;
        }

        @Override
        public String toString() {
            return "Vertex{" +
                    "vertexId='" + vertexId + '\'' +
                    ", visible=" + visible +
                    '}';
        }
    }

    public static class User {
        private String userId;
        private WorkspaceAccess access;

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public WorkspaceAccess getAccess() {
            return access;
        }

        public void setAccess(WorkspaceAccess access) {
            this.access = access;
        }

        @Override
        public String toString() {
            return "User{" +
                    "userId='" + userId + '\'' +
                    ", access=" + access +
                    '}';
        }
    }
}
