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
package com.mware.core.ingest.structured.mapping;

import com.mware.core.exception.BcException;
import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.ingest.structured.model.ClientApiMappingErrors;
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.ge.Authorizations;
import com.mware.ge.Visibility;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

import java.util.Map;

public class PropertyMapping {
    public static final String PROPERTY_MAPPING_NAME_KEY = "name";
    public static final String PROPERTY_MAPPING_VALUE_KEY = "value";
    public static final String PROPERTY_MAPPING_KEY_KEY = "key";
    public static final String PROPERTY_MAPPING_IS_IDENTIFIER_KEY = "isIdentifier";
    public static final String PROPERTY_MAPPING_ERROR_STRATEGY_KEY = "errorStrategy";
    public static final String PROPERTY_MAPPING_VISIBILITY_KEY = "visibilitySource";

    public enum ErrorHandlingStrategy {
        SET_CELL_ERROR_PROPERTY,
        SKIP_CELL,
        SKIP_VERTEX,
        SKIP_ROW
    }
    public String key;
    public String name;
    public String value;
    public boolean identifier;
    public ErrorHandlingStrategy errorHandlingStrategy;
    public VisibilityJson visibilityJson;
    public Visibility visibility;

    public PropertyMapping() {
    }

    public PropertyMapping(VisibilityTranslator visibilityTranslator, String workspaceId, JSONObject propertyMapping) {
        this.name = propertyMapping.getString(PROPERTY_MAPPING_NAME_KEY);
        this.identifier = propertyMapping.optBoolean(PROPERTY_MAPPING_IS_IDENTIFIER_KEY, false);

        String visibilitySource = propertyMapping.optString(PROPERTY_MAPPING_VISIBILITY_KEY);
        if(!StringUtils.isBlank(visibilitySource)) {
            visibilityJson = new VisibilityJson(visibilitySource);
            visibilityJson.addWorkspace(workspaceId);
            visibility = visibilityTranslator.toVisibility(visibilityJson).getVisibility();
        }

        if(propertyMapping.has(PROPERTY_MAPPING_ERROR_STRATEGY_KEY)) {
            this.errorHandlingStrategy = ErrorHandlingStrategy.valueOf(propertyMapping.getString(PROPERTY_MAPPING_ERROR_STRATEGY_KEY));
        }

        this.value = propertyMapping.optString(PROPERTY_MAPPING_VALUE_KEY);
        this.key = propertyMapping.optString(PROPERTY_MAPPING_KEY_KEY);
    }

    public PropertyMapping(VisibilityTranslator visibilityTranslator, String workspaceId, ClientApiDataSource.EntityMapping mapping) {
        this.name = mapping.getColProperty();
        this.identifier = mapping.isColIdentifier();

        if(!StringUtils.isEmpty(mapping.getColPropVisibility())) {
            visibilityJson = new VisibilityJson(mapping.getColPropVisibility());
            visibilityJson.addWorkspace(workspaceId);
            visibility = visibilityTranslator.toVisibility(visibilityJson).getVisibility();
        }

        this.key = mapping.colName;
    }

    public Object extractRawValue(Map<String, Object> row) {
        if(!StringUtils.isBlank(value)) {
            return value;
        } else {
            return row.get(this.key);
        }
    }

    public Value decodeValue(Map<String, Object> row) {
        return decodeValue(extractRawValue(row));
    }

    public Value decodeValue(Object rawPropertyValue) {
        if (rawPropertyValue instanceof String) {
            return StringUtils.isBlank((String)rawPropertyValue) ? null : Values.stringValue((String) rawPropertyValue);
        }
        return Values.of(rawPropertyValue);
    }

    public static PropertyMapping fromJSON(SchemaRepository schemaRepository, VisibilityTranslator visibilityTranslator, String workspaceId, JSONObject propertyMapping) {
        String propertyName = propertyMapping.getString(PROPERTY_MAPPING_NAME_KEY);

        if (!VertexMapping.CONCEPT_TYPE.equals(propertyName)) {
            SchemaProperty schemaProperty = schemaRepository.getPropertyByName(propertyName, workspaceId);
            if (schemaProperty == null) {
                throw new BcException("Property " + propertyName + " was not found in the ontology.");
            }

            if (schemaProperty.getDataType() == PropertyType.DATE) {
                return new DatePropertyMapping(visibilityTranslator, workspaceId, propertyMapping);
            } else if (schemaProperty.getDataType() == PropertyType.BOOLEAN) {
                return new BooleanPropertyMapping(visibilityTranslator, workspaceId, propertyMapping);
            } else if (schemaProperty.getDataType() == PropertyType.GEO_LOCATION) {
                return new GeoPointPropertyMapping(visibilityTranslator, workspaceId, propertyMapping);
            } else if (schemaProperty.getDataType() == PropertyType.CURRENCY ||
                    schemaProperty.getDataType() == PropertyType.DOUBLE ||
                    schemaProperty.getDataType() == PropertyType.INTEGER) {
                return new NumericPropertyMapping(schemaProperty, visibilityTranslator, workspaceId, propertyMapping);
            }
        }

        return new PropertyMapping(visibilityTranslator, workspaceId, propertyMapping);
    }

    public ClientApiMappingErrors validate(Authorizations authorizations) {
        ClientApiMappingErrors errors = new ClientApiMappingErrors();

        if(visibility != null && !authorizations.canRead(visibility)) {
            ClientApiMappingErrors.MappingError mappingError = new ClientApiMappingErrors.MappingError();
            mappingError.propertyMapping = this;
            mappingError.attribute = PROPERTY_MAPPING_VISIBILITY_KEY;
            mappingError.message = "Invalid visibility specified.";
            errors.mappingErrors.add(mappingError);
        }

        return errors;
    }

    public static PropertyMapping fromDataSourceImport(SchemaRepository ontologyRepository, VisibilityTranslator visibilityTranslator, String workspaceId, ClientApiDataSource.EntityMapping mapping) {
        String propertyName = mapping.getColProperty();
        SchemaProperty schemaProperty = ontologyRepository.getPropertyByName(propertyName, workspaceId);

        if (schemaProperty == null) {
            throw new BcException("Property " + propertyName + " was not found in the ontology.");
        }

        if (schemaProperty.getDataType() == PropertyType.DATE) {
            return new DatePropertyMapping(visibilityTranslator, workspaceId, mapping);
        } else if (schemaProperty.getDataType() == PropertyType.BOOLEAN) {
            return new BooleanPropertyMapping(visibilityTranslator, workspaceId, mapping);
        } else if (schemaProperty.getDataType() == PropertyType.GEO_LOCATION) {
            return new GeoPointPropertyMapping(visibilityTranslator, workspaceId, mapping);
        } else if (schemaProperty.getDataType() == PropertyType.CURRENCY ||
                schemaProperty.getDataType() == PropertyType.DOUBLE ||
                schemaProperty.getDataType() == PropertyType.INTEGER) {
            return new NumericPropertyMapping(schemaProperty, visibilityTranslator, workspaceId, mapping);
        }
        return new PropertyMapping(visibilityTranslator, workspaceId, mapping);
    }
}
