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
package com.mware.ge.elasticsearch5;

import com.mware.ge.*;
import com.mware.ge.elasticsearch5.utils.Ascii85;
import com.mware.ge.elasticsearch5.utils.Murmur3;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

public class IdStrategy {
    public static final String ELEMENT_TYPE = "e";
    private static final String EXTENDED_DATA_FIELD_SEPARATOR = ":";

    public String getType() {
        return ELEMENT_TYPE;
    }

    public String createExtendedDataDocId(ElementLocation elementLocation, String tableName, String rowId) {
        return createExtendedDataDocId(elementLocation.getId(), tableName, rowId);
    }

    public String createExtendedDataDocId(ExtendedDataRowId rowId) {
        return createExtendedDataDocId(rowId.getElementId(), rowId.getTableName(), rowId.getRowId());
    }

    public String createExtendedDataDocId(String elementId, String tableName, String rowId) {
        return createDocId(elementId + EXTENDED_DATA_FIELD_SEPARATOR + tableName + EXTENDED_DATA_FIELD_SEPARATOR + rowId);
    }

    public String createElementDocId(ElementId elementId) {
        return createDocId(elementId.getId());
    }

    private String createDocId(String s) {
        byte[] hash = Murmur3.hash128(s.getBytes());
        return Ascii85.encode(hash);
    }

    public ExtendedDataRowId extendedDataRowIdFromSearchHit(SearchHit hit) {
        DocumentField elementTypeField = hit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME);
        if (elementTypeField == null) {
            throw new GeException("Could not find field: " + Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME);
        }
        ElementType elementType = ElasticsearchDocumentType.parse(elementTypeField.getValue().toString()).toElementType();

        DocumentField elementIdField = hit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME);
        if (elementIdField == null) {
            throw new GeException("Could not find field: " + Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME);
        }
        String elementId = elementIdField.getValue();

        DocumentField tableNameField = hit.getFields().get(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME);
        if (tableNameField == null) {
            throw new GeException("Could not find field: " + Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_NAME_FIELD_NAME);
        }
        String tableName = tableNameField.getValue();

        DocumentField rowIdField = hit.getFields().get(Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME);
        if (rowIdField == null) {
            throw new GeException("Could not find field: " + Elasticsearch5SearchIndex.EXTENDED_DATA_TABLE_ROW_ID_FIELD_NAME);
        }
        String rowId = rowIdField.getValue();

        return new ExtendedDataRowId(elementType, elementId, tableName, rowId);
    }

    public String vertexIdFromSearchHit(SearchHit hit) {
        return elementIdFromSearchHit(hit);
    }

    public String edgeIdFromSearchHit(SearchHit hit) {
        return elementIdFromSearchHit(hit);
    }

    private String elementIdFromSearchHit(SearchHit hit) {
        DocumentField elementIdField = hit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME);
        if (elementIdField == null) {
            throw new GeException("Could not find field: " + Elasticsearch5SearchIndex.ELEMENT_ID_FIELD_NAME);
        }
        return elementIdField.getValue();
    }

    public Object fromSearchHit(SearchHit hit) {
        ElasticsearchDocumentType dt = ElasticsearchDocumentType.fromSearchHit(hit);
        if (dt == null) {
            return null;
        }
        switch (dt) {
            case EDGE:
                return edgeIdFromSearchHit(hit);
            case VERTEX:
                return vertexIdFromSearchHit(hit);
            case EDGE_EXTENDED_DATA:
            case VERTEX_EXTENDED_DATA:
                return extendedDataRowIdFromSearchHit(hit);
            default:
                throw new GeException("Unhandled document type: " + dt);
        }
    }
}
