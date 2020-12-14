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
package com.mware.core.user;

import com.mware.ge.values.storable.Value;
import org.json.JSONObject;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.clientapi.dto.UserStatus;
import com.mware.core.model.clientapi.dto.UserType;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SystemUser implements User {
    private static final long serialVersionUID = 1L;
    public static final String USERNAME = "system";
    public static final String USER_ID = UserRepository.GRAPH_USER_ID_PREFIX + "system";

    public SystemUser() {
    }

    @Override
    public String getUserId() {
        return USER_ID;
    }

    @Override
    public String getUsername() {
        return USERNAME;
    }

    @Override
    public String getDisplayName() {
        return USERNAME;
    }

    @Override
    public String getEmailAddress() {
        return USERNAME;
    }

    @Override
    public ZonedDateTime getCreateDate() {
        return ZonedDateTime.now().minus(10, ChronoUnit.YEARS);
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
        return UserType.SYSTEM;
    }

    @Override
    public UserStatus getUserStatus() {
        return UserStatus.OFFLINE;
    }

    @Override
    public String getCurrentWorkspaceId() {
        return null;
    }

    @Override
    public JSONObject getUiPreferences() {
        return new JSONObject();
    }

    @Override
    public String toString() {
        return "SystemUser";
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
        return null;
    }

    @Override
    public Map<String, Value> getCustomProperties() {
        return new HashMap<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SystemUser that = (SystemUser) o;

        if (!getUserId().equals(that.getUserId())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getUserId().hashCode();
    }
}
