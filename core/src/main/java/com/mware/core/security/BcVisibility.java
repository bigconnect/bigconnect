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
package com.mware.core.security;

import com.mware.ge.Visibility;
import org.apache.commons.lang.StringUtils;

public class BcVisibility {
    public static final String SUPER_USER_VISIBILITY_STRING = "administrator";
    private final Visibility visibility;

    public BcVisibility() {
        this.visibility = new Visibility("");
    }

    public BcVisibility(String visibility) {
        if (visibility == null || visibility.length() == 0) {
            this.visibility = new Visibility("");
        } else {
            this.visibility = addSuperUser(visibility);
        }
    }

    public BcVisibility(Visibility visibility) {
        if (visibility == null || visibility.getVisibilityString().length() == 0
                || visibility.getVisibilityString().contains(SUPER_USER_VISIBILITY_STRING)) {
            this.visibility = visibility;
        } else {
            this.visibility = addSuperUser(visibility.getVisibilityString());
        }
    }

    public Visibility getVisibility() {
        return visibility;
    }

    private Visibility addSuperUser(String visibility) {
        return new Visibility("(" + visibility + ")|" + SUPER_USER_VISIBILITY_STRING);
    }

    public Visibility orVisibility(String visibility) {
        if (StringUtils.isEmpty(this.visibility.getVisibilityString())) {
            return new Visibility("(" + visibility + ")");
        }
        return new Visibility(this.visibility.getVisibilityString() + "|" + visibility);
    }

    @Override
    public String toString() {
        return getVisibility().toString();
    }

    public static Visibility and(Visibility visibility, String additionalVisibility) {
        if (visibility.getVisibilityString().length() == 0) {
            return new Visibility(additionalVisibility);
        }
        return new Visibility("(" + visibility.getVisibilityString() + ")&(" + additionalVisibility + ")");
    }

    public static Visibility or(Visibility visibility, String additionalVisibility) {
        if (visibility.getVisibilityString().length() == 0) {
            return new Visibility(additionalVisibility);
        }
        return new Visibility("(" + visibility.getVisibilityString() + ")|(" + additionalVisibility + ")");
    }
}
