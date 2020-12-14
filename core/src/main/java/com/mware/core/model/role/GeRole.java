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
package com.mware.core.model.role;

import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.properties.RoleSchema;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.values.storable.Value;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GeRole implements Role, Serializable {
    private static final long serialVersionUID = 1L;
    private final String roleId;
    private final Map<String, Value> properties = new HashMap<>();

    public GeRole(Vertex roleVertex) {
        this.roleId = roleVertex.getId();
        for (Property property : roleVertex.getProperties()) {
            this.properties.put(property.getName(), property.getValue());
        }
    }

    @Override
    public String getRoleId() {
        return roleId;
    }

    @Override
    public String getRoleName() {
        return RoleSchema.ROLE_NAME.getPropertyValue(properties);
    }

    @Override
    public String getDescription() {
        return RoleSchema.DESCRIPTION.getPropertyValue(properties);
    }

    @Override
    public Boolean isGlobal() {
        return RoleSchema.GLOBAL.getPropertyValue(properties);
    }

    @Override
    public Set<Privilege> getPrivileges() {
        String privs = RoleSchema.PRIVILEGES.getPropertyValue(properties);
        if(privs != null) {
            Set<String> set = Privilege.stringToPrivileges(privs);
            return set.stream()
                    .map(privilegeName -> new Privilege(privilegeName))
                    .collect(Collectors.toSet());
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof Role && ((Role)obj).getRoleName().equals(getRoleName()));
    }
}
