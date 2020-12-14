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
package com.mware.core.model.user;

import com.google.common.collect.ImmutableMap;
import com.mware.core.model.clientapi.dto.UserStatus;
import com.mware.core.model.clientapi.dto.UserType;
import com.mware.core.user.User;
import com.mware.ge.values.storable.Value;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class InMemoryUser implements User {
    private final String userId;
    private final String userName;
    private final String displayName;
    private final String emailAddress;
    private final ZonedDateTime createDate;
    private final String currentWorkspaceId;
    private JSONObject preferences;
    private Map<String, Value> properties = new HashMap<>();

    public InMemoryUser(String userId) {
        this(userId, null, null, null, null);
    }

    public InMemoryUser(
            String userName,
            String displayName,
            String emailAddress,
            String currentWorkspaceId
    ) {
        this(UUID.randomUUID().toString(), userName, displayName, emailAddress, currentWorkspaceId);
    }

    public InMemoryUser(
            String userId,
            String userName,
            String displayName,
            String emailAddress,
            String currentWorkspaceId
    ) {
        this.userId = userId;
        this.userName = userName;
        this.displayName = displayName;
        this.emailAddress = emailAddress;
        this.createDate = ZonedDateTime.now();
        this.currentWorkspaceId = currentWorkspaceId;
        this.preferences = new JSONObject();
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public String getUsername() {
        return userName;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getEmailAddress() {
        return emailAddress;
    }

    @Override
    public ZonedDateTime getCreateDate() {
        return createDate;
    }

    @Override
    public ZonedDateTime getCurrentLoginDate() {
        return null;
    }

    @Override
    public String getCurrentLoginRemoteAddr() {
        return null;
    }

    @Override
    public ZonedDateTime getPreviousLoginDate() {
        return null;
    }

    @Override
    public String getPreviousLoginRemoteAddr() {
        return null;
    }

    @Override
    public int getLoginCount() {
        return 0;
    }

    @Override
    public UserType getUserType() {
        return UserType.USER;
    }

    @Override
    public UserStatus getUserStatus() {
        return UserStatus.OFFLINE;
    }

    @Override
    public String getCurrentWorkspaceId() {
        return this.currentWorkspaceId;
    }

    @Override
    public JSONObject getUiPreferences() {
        return preferences;
    }

    public void setPreferences(JSONObject preferences) {
        this.preferences = preferences;
    }

    @Override
    public String getPasswordResetToken() {
        return null;
    }

    @Override
    public ZonedDateTime getPasswordResetTokenExpirationDate() {
        return null;
    }

    @Override
    public Value getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    @Override
    public Map<String, Value> getCustomProperties() {
        return ImmutableMap.copyOf(properties);
    }

    public void setProperty(String propertyName, Value value) {
        properties.put(propertyName, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InMemoryUser that = (InMemoryUser) o;

        if (!userId.equals(that.userId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return userId.hashCode();
    }

    @Override
    public String toString() {
        return "InMemoryUser{" +
                "userId='" + userId + '\'' +
                '}';
    }
}
