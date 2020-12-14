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
import com.mware.core.model.properties.UserSchema;
import com.mware.core.user.User;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.values.storable.Value;
import org.json.JSONObject;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.mware.ge.values.storable.Values.stringValue;

public class GeUser implements User, Serializable {
    private static final long serialVersionUID = 6688073934273514248L;
    private final String userId;
    private final Map<String, Value> properties = new HashMap<>();

    public GeUser(Vertex userVertex) {
        this.userId = userVertex.getId();
        for (Property property : userVertex.getProperties()) {
            this.properties.put(property.getName(), property.getValue());
        }
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public String getUsername() {
        return UserSchema.USERNAME.getPropertyValue(properties);
    }

    @Override
    public String getDisplayName() {
        return UserSchema.DISPLAY_NAME.getPropertyValue(properties);
    }

    @Override
    public String getEmailAddress() {
        return UserSchema.EMAIL_ADDRESS.getPropertyValue(properties);
    }

    @Override
    public ZonedDateTime getCreateDate() {
        return UserSchema.CREATE_DATE.getPropertyValue(properties);
    }

    @Override
    public ZonedDateTime getCurrentLoginDate() {
        return UserSchema.CURRENT_LOGIN_DATE.getPropertyValue(properties);
    }

    @Override
    public String getCurrentLoginRemoteAddr() {
        return UserSchema.CURRENT_LOGIN_REMOTE_ADDR.getPropertyValue(properties);
    }

    @Override
    public ZonedDateTime getPreviousLoginDate() {
        return UserSchema.PREVIOUS_LOGIN_DATE.getPropertyValue(properties);
    }

    @Override
    public String getPreviousLoginRemoteAddr() {
        return UserSchema.PREVIOUS_LOGIN_REMOTE_ADDR.getPropertyValue(properties);
    }

    @Override
    public int getLoginCount() {
        return UserSchema.LOGIN_COUNT.getPropertyValue(properties, 0);
    }

    @Override
    public UserType getUserType() {
        return UserType.USER;
    }

    @Override
    public UserStatus getUserStatus() {
        return UserStatus.valueOf(UserSchema.STATUS.getPropertyValue(properties));
    }

    public void setUserStatus(UserStatus status) {
        UserSchema.STATUS.setProperty(properties, status.name());
    }

    @Override
    public String getCurrentWorkspaceId() {
        return UserSchema.CURRENT_WORKSPACE.getPropertyValue(properties);
    }

    @Override
    public JSONObject getUiPreferences() {
        JSONObject preferences = UserSchema.UI_PREFERENCES.getPropertyValue(properties);
        if (preferences == null) {
            preferences = new JSONObject();
            UserSchema.UI_PREFERENCES.setProperty(properties, preferences);
        }
        return preferences;
    }

    @Override
    public String getPasswordResetToken() {
        return UserSchema.PASSWORD_RESET_TOKEN.getPropertyValue(properties);
    }

    @Override
    public ZonedDateTime getPasswordResetTokenExpirationDate() {
        return UserSchema.PASSWORD_RESET_TOKEN_EXPIRATION_DATE.getPropertyValue(properties);
    }

    @Override
    public Value getProperty(String propertyName) {
        return this.properties.get(propertyName);
    }

    @Override
    public Map<String, Value> getCustomProperties() {
        Map<String, Value> results = new HashMap<>();
        for (Map.Entry<String, Value> property : properties.entrySet()) {
            if (!UserSchema.isBuiltInProperty(property.getKey())) {
                results.put(property.getKey(), property.getValue());
            }
        }
        return ImmutableMap.copyOf(results);
    }

    public void setProperty(String propertyName, Value value) {
        this.properties.put(propertyName, value);
    }

    @Override
    public String toString() {
        return "GeUser{userId='" + getUserId() + "', displayName='" + getDisplayName() + "}";
    }
}
