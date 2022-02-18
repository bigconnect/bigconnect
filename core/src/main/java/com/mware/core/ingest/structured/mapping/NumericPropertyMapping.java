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
import com.mware.core.model.clientapi.dto.PropertyType;
import com.mware.core.model.schema.SchemaProperty;
import com.mware.core.security.VisibilityTranslator;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.json.JSONObject;

import java.text.NumberFormat;
import java.text.ParseException;

public class NumericPropertyMapping extends PropertyMapping {
    private NumberFormat numberFormat;

    public NumericPropertyMapping(SchemaProperty schemaProperty) {
        numberFormat = ontologyToNumberFormat(schemaProperty);
    }

    public NumericPropertyMapping(
            SchemaProperty schemaProperty, VisibilityTranslator visibilityTranslator, String workspaceId, JSONObject propertyMapping) {
        super(visibilityTranslator, workspaceId, propertyMapping);
        numberFormat = ontologyToNumberFormat(schemaProperty);
    }

    public NumericPropertyMapping(SchemaProperty schemaProperty, VisibilityTranslator visibilityTranslator, String workspaceId, ClientApiDataSource.EntityMapping mapping) {
        super(visibilityTranslator, workspaceId, mapping);
        numberFormat = ontologyToNumberFormat(schemaProperty);
    }

    private NumberFormat ontologyToNumberFormat(SchemaProperty schemaProperty) {
        PropertyType dataType = schemaProperty.getDataType();
        if(dataType == PropertyType.INTEGER) {
            return NumberFormat.getIntegerInstance();
        } else {
            return NumberFormat.getNumberInstance();
        }
    }

    @Override
    public Value decodeValue(Object rawPropertyValue) {
        if (rawPropertyValue instanceof String) {
            String value = (String) rawPropertyValue;
            if (!StringUtils.isBlank(value.replaceAll("\\D", ""))) {
                try {
                    return Values.numberValue(numberFormat.parse(
                            value
                            .replaceAll("[^\\d\\.,\\-]", "")
                            .replaceAll("(?<!^)\\-", "")
                    ));
                } catch (ParseException pe) {
                    try {
                        return Values.numberValue(NumberUtils.createNumber(value));
                    } catch (NumberFormatException ex) {
                        throw new BcException("Unrecognized number format: " + rawPropertyValue, pe);
                    }
                }
            } else return null;
        }
        return Values.of(rawPropertyValue);
    }
}
