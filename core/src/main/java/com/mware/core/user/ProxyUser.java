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
import java.util.Date;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class is used to store the userId only in a web session. If we were to store the entire
 * user object in the session, any changes to the user would not be reflected unless the user object
 * was refreshed.
 */
public class ProxyUser implements User {
    private static final long serialVersionUID = -7652656758524792116L;
    private final String userId;
    private final UserRepository userRepository;
    private User proxiedUser;

    public ProxyUser(String userId, UserRepository userRepository) {
        checkNotNull(userId, "userId cannot be null");
        checkNotNull(userRepository, "userRepository cannot be null");
        this.userId = userId;
        this.userRepository = userRepository;
    }

    @Override
    public String getUserId() {
        return userId;
    }

    public User getProxiedUser() {
        ensureUser();
        return proxiedUser;
    }

    @Override
    public String getUsername() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getUsername();
    }

    @Override
    public String getDisplayName() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getDisplayName();
    }

    @Override
    public String getEmailAddress() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getEmailAddress();
    }

    @Override
    public ZonedDateTime getCreateDate() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getCreateDate();
    }

    @Override
    public ZonedDateTime getCurrentLoginDate() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getCurrentLoginDate();
    }

    @Override
    public String getCurrentLoginRemoteAddr() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getCurrentLoginRemoteAddr();
    }

    @Override
    public ZonedDateTime getPreviousLoginDate() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getPreviousLoginDate();
    }

    @Override
    public String getPreviousLoginRemoteAddr() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getPreviousLoginRemoteAddr();
    }

    @Override
    public int getLoginCount() {
        ensureUser();
        if (proxiedUser == null) {
            return 0;
        }
        return proxiedUser.getLoginCount();
    }

    @Override
    public UserType getUserType() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getUserType();
    }

    @Override
    public UserStatus getUserStatus() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getUserStatus();
    }

    @Override
    public String getCurrentWorkspaceId() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getCurrentWorkspaceId();
    }

    @Override
    public JSONObject getUiPreferences() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getUiPreferences();
    }

    @Override
    public String getPasswordResetToken() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getPasswordResetToken();
    }

    @Override
    public ZonedDateTime getPasswordResetTokenExpirationDate() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getPasswordResetTokenExpirationDate();
    }

    @Override
    public Value getProperty(String propertyName) {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getProperty(propertyName);
    }

    @Override
    public Map<String, Value> getCustomProperties() {
        ensureUser();
        if (proxiedUser == null) {
            return null;
        }
        return proxiedUser.getCustomProperties();
    }

    private void ensureUser() {
        if (proxiedUser == null) {
            proxiedUser = userRepository.findById(userId);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof User)) {
            return false;
        }

        User other = (User) o;
        return getUserId().equals(other.getUserId());
    }

    @Override
    public int hashCode() {
        return getUserId().hashCode();
    }

    @Override
    public String toString() {
        return "ProxyUser{" +
                "userId='" + userId + '\'' +
                '}';
    }
}
