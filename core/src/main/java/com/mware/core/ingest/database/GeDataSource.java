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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.model.clientapi.util.ClientApiConverter;
import com.mware.core.model.clientapi.util.ObjectMapperFactory;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.values.storable.Value;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeDataSource implements DataSource, Serializable {
    private static final long serialVersionUID = 1L;
    private final String id;
    private final Map<String, Value> properties = new HashMap<>();

    public GeDataSource(Vertex vertex) {
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
        return DataConnectionSchema.DS_NAME.getPropertyValue(properties);
    }

    @Override
    public String getDescription() {
        return DataConnectionSchema.DS_DESCRIPTION.getPropertyValue(properties);
    }

    @Override
    public Long getMaxRecords() {
        return DataConnectionSchema.DS_MAX_RECORDS.getPropertyValue(properties);
    }

    @Override
    public String getSqlSelect() {
        return DataConnectionSchema.DS_SQL.getPropertyValue(properties);
    }

    @Override
    public ZonedDateTime getLastImportDate() {
        return DataConnectionSchema.DS_LAST_IMPORT_DATE.getPropertyValue(properties);
    }

    @Override
    public boolean isRunning() {
        Boolean value = DataConnectionSchema.DS_IMPORT_RUNNING.getPropertyValue(properties);
        return value != null ? value : false;
    }

    @Override
    public List<ClientApiDataSource.EntityMapping> getEntityMapping() {
        String json = DataConnectionSchema.DS_ENTITY_MAPPING.getPropertyValue(properties);
        ObjectMapper mapper = ObjectMapperFactory.getInstance();
        try {
            return ObjectMapperFactory.getInstance()
                    .readValue(json, mapper.getTypeFactory().constructCollectionType(List.class, ClientApiDataSource.EntityMapping.class));
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public List<ClientApiDataSource.RelMapping> getRelMapping() {
        String json = DataConnectionSchema.DS_RELATIONSHIP_MAPPING.getPropertyValue(properties);
        if(StringUtils.isEmpty(json))
            return new ArrayList<>();

        ObjectMapper mapper = ObjectMapperFactory.getInstance();
        try {
            return ObjectMapperFactory.getInstance()
                            .readValue(json, mapper.getTypeFactory().constructCollectionType(List.class, ClientApiDataSource.RelMapping.class));
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public ClientApiDataSource.ImportConfig getImportConfig() {
        String value = DataConnectionSchema.DS_IMPORT_CONFIG.getPropertyValue(properties);
        if(!StringUtils.isBlank(value)) {
            return ClientApiConverter.toClientApi(
                    DataConnectionSchema.DS_IMPORT_CONFIG.getPropertyValue(properties),
                    ClientApiDataSource.ImportConfig.class
            );
        } else
            return null;
    }
}
