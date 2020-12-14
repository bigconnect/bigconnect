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
package com.mware.core.model.properties;

import com.mware.core.model.properties.types.*;

public final class UserSchema {
    public static final StringSingleValueBcProperty AUTHORIZATIONS = new StringSingleValueBcProperty("userAuthorizations");
    public static final StringSingleValueBcProperty USERNAME = new StringSingleValueBcProperty("userUsername");
    public static final StringSingleValueBcProperty DISPLAY_NAME = new StringSingleValueBcProperty("userDisplayName");
    public static final StringSingleValueBcProperty EMAIL_ADDRESS = new StringSingleValueBcProperty("userEmailAddress");
    public static final DateSingleValueBcProperty CREATE_DATE = new DateSingleValueBcProperty("userCreateDate");
    public static final DateSingleValueBcProperty CURRENT_LOGIN_DATE = new DateSingleValueBcProperty("userCurrentLoginDate");
    public static final StringSingleValueBcProperty CURRENT_LOGIN_REMOTE_ADDR = new StringSingleValueBcProperty("userCurrentLoginRemoteAddr");
    public static final DateSingleValueBcProperty PREVIOUS_LOGIN_DATE = new DateSingleValueBcProperty("userPreviousLoginDate");
    public static final StringSingleValueBcProperty PREVIOUS_LOGIN_REMOTE_ADDR = new StringSingleValueBcProperty("userPreviousLoginRemoteAddr");
    public static final IntegerSingleValueBcProperty LOGIN_COUNT = new IntegerSingleValueBcProperty("userLoginCount");
    public static final StringSingleValueBcProperty STATUS = new StringSingleValueBcProperty("userStatus");
    public static final StringSingleValueBcProperty CURRENT_WORKSPACE = new StringSingleValueBcProperty("userCurrentWorkspace");
    public static final JsonSingleValueBcProperty UI_PREFERENCES = new JsonSingleValueBcProperty("userUiPreferences");
    public static final ByteArraySingleValueBcProperty PASSWORD_SALT = new ByteArraySingleValueBcProperty("userPasswordSalt");
    public static final ByteArraySingleValueBcProperty PASSWORD_HASH = new ByteArraySingleValueBcProperty("userPasswordHash");
    public static final StringSingleValueBcProperty PASSWORD_RESET_TOKEN = new StringSingleValueBcProperty("userPasswordResetToken");
    public static final DateSingleValueBcProperty PASSWORD_RESET_TOKEN_EXPIRATION_DATE = new DateSingleValueBcProperty("userPasswordResetTokenExpirationDate");

    public static boolean isBuiltInProperty(String propertyName) {
        return BcSchema.isBuiltInProperty(propertyName)
                || BcSchema.isBuiltInProperty(UserSchema.class, propertyName);
    }
}
