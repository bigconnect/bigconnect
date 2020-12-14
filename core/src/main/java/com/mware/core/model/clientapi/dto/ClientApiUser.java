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

import com.fasterxml.jackson.databind.JsonNode;
import com.mware.core.model.clientapi.util.ClientApiConverter;

import java.time.ZonedDateTime;
import java.util.*;

public class ClientApiUser implements ClientApiObject {
    private String id;
    private String userName;
    private String displayName;
    private UserType userType;
    private String currentWorkspaceId;
    private UserStatus status;
    private String email;
    private String currentWorkspaceName;
    private String csrfToken;
    private ZonedDateTime currentLoginDate;
    private ZonedDateTime previousLoginDate;
    private Integer sessionCount = null;
    private Set<String> privileges = new HashSet<String>();
    private JsonNode uiPreferences;
    private List<String> authorizations = new ArrayList<String>();
    private List<Object> longRunningProcesses = new ArrayList<Object>();
    private List<ClientApiWorkspace> workspaces = new ArrayList<ClientApiWorkspace>();
    private Map<String, Object> properties = new HashMap<String, Object>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Integer getSessionCount() {
        return sessionCount;
    }

    public void setSessionCount(Integer sessionCount) {
        this.sessionCount = sessionCount;
    }

    public UserType getUserType() {
        return userType;
    }

    public void setUserType(UserType userType) {
        this.userType = userType;
    }

    public String getCurrentWorkspaceId() {
        return currentWorkspaceId;
    }

    public void setCurrentWorkspaceId(String currentWorkspaceId) {
        this.currentWorkspaceId = currentWorkspaceId;
    }

    public UserStatus getStatus() {
        return status;
    }

    public void setStatus(UserStatus status) {
        this.status = status;
    }

    public ZonedDateTime getCurrentLoginDate() {
        return currentLoginDate;
    }

    public void setCurrentLoginDate(ZonedDateTime currentLoginDate) {
        this.currentLoginDate = currentLoginDate;
    }

    public ZonedDateTime getPreviousLoginDate() {
        return previousLoginDate;
    }

    public void setPreviousLoginDate(ZonedDateTime previousLoginDate) {
        this.previousLoginDate = previousLoginDate;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getEmail() {
        return email;
    }

    public void setCurrentWorkspaceName(String currentWorkspaceName) {
        this.currentWorkspaceName = currentWorkspaceName;
    }

    public String getCurrentWorkspaceName() {
        return currentWorkspaceName;
    }

    public String getCsrfToken() {
        return csrfToken;
    }

    public void setCsrfToken(String csrfToken) {
        this.csrfToken = csrfToken;
    }

    public JsonNode getUiPreferences() {
        return uiPreferences;
    }

    public void setUiPreferences(JsonNode uiPreferences) {
        this.uiPreferences = uiPreferences;
    }

    public Set<String> getPrivileges() {
        return privileges;
    }

    public List<String> getAuthorizations() {
        return authorizations;
    }

    public void addAuthorization(String auth) {
        this.authorizations.add(auth);
    }

    public List<Object> getLongRunningProcesses() {
        return longRunningProcesses;
    }

    public List<ClientApiWorkspace> getWorkspaces() {
        return workspaces;
    }

    @Override
    public String toString() {
        return ClientApiConverter.clientApiToString(this);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
