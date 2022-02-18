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
 *
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
package com.mware.core.ingest.database;

import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.values.storable.Value;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class GeDataConnection implements DataConnection, Serializable {
    private static final long serialVersionUID = 1L;
    private final String id;
    private final Map<String, Value> properties = new HashMap<>();

    public GeDataConnection(Vertex vertex) {
        this.id = vertex.getId();
        for (Property property : vertex.getProperties()) {
            this.properties.put(property.getName(), property.getValue());
        }
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return DataConnectionSchema.DC_NAME.getPropertyValue(properties);
    }

    @Override
    public String getDescription() {
        return DataConnectionSchema.DC_DESCRIPTION.getPropertyValue(properties);
    }

    @Override
    public String getDriverClass() {
        return DataConnectionSchema.DC_DRIVER_CLASS.getPropertyValue(properties);
    }

    @Override
    public String getJdbcUrl() {
        return DataConnectionSchema.DC_JDBC_URL.getPropertyValue(properties);
    }

    @Override
    public String getDriverProperties() {
        return DataConnectionSchema.DC_DRIVER_PROPS.getPropertyValue(properties);
    }

    @Override
    public String getUsername() {
        return DataConnectionSchema.DC_USERNAME.getPropertyValue(properties);
    }

    @Override
    public String getPassword() {
        return DataConnectionSchema.DC_PASSWORD.getPropertyValue(properties);
    }
}
