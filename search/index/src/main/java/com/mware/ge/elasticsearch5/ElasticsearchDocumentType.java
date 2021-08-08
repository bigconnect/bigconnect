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

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import com.mware.ge.*;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public enum ElasticsearchDocumentType {
    VERTEX("vertex"),
    EDGE("edge"),
    VERTEX_EXTENDED_DATA("vertexextdata"),
    EDGE_EXTENDED_DATA("edgeextdata");

    public static final EnumSet<ElasticsearchDocumentType> ELEMENTS = EnumSet.of(VERTEX, EDGE);
    private final String key;

    ElasticsearchDocumentType(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public static ElasticsearchDocumentType parse(String s) {
        if (s.equals(VERTEX.getKey())) {
            return VERTEX;
        } else if (s.equals(EDGE.getKey())) {
            return EDGE;
        } else if (s.equals(VERTEX_EXTENDED_DATA.getKey())) {
            return VERTEX_EXTENDED_DATA;
        } else if (s.equals(EDGE_EXTENDED_DATA.getKey())) {
            return EDGE_EXTENDED_DATA;
        }
        throw new GeException("Could not parse element type: " + s);
    }

    public ElementType toElementType() {
        switch (this) {
            case VERTEX:
            case VERTEX_EXTENDED_DATA:
                return ElementType.VERTEX;
            case EDGE:
            case EDGE_EXTENDED_DATA:
                return ElementType.EDGE;
        }
        throw new GeException("Unhandled type: " + this);
    }

    public static EnumSet<ElasticsearchDocumentType> fromGeObjectTypes(EnumSet<GeObjectType> objectTypes) {
        List<ElasticsearchDocumentType> enums = new ArrayList<>();
        for (GeObjectType objectType : objectTypes) {
            switch (objectType) {
                case VERTEX:
                    enums.add(VERTEX);
                    break;
                case EDGE:
                    enums.add(EDGE);
                    break;
                case EXTENDED_DATA:
                    enums.add(VERTEX_EXTENDED_DATA);
                    enums.add(EDGE_EXTENDED_DATA);
                    break;
                case STREAMING_DATA:
                    break;
                default:
                    throw new GeException("Unhandled Ge object type: " + objectType);
            }
        }
        return EnumSet.copyOf(enums);
    }

    public static ElasticsearchDocumentType fromSearchHit(SearchHit searchHit) {
        DocumentField elementType = searchHit.getFields().get(Elasticsearch5SearchIndex.ELEMENT_TYPE_FIELD_NAME);
        if (elementType == null) {
            return null;
        }
        return ElasticsearchDocumentType.parse(elementType.getValue().toString());
    }

    public static ElasticsearchDocumentType getExtendedDataDocumentTypeFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return VERTEX_EXTENDED_DATA;
            case EDGE:
                return EDGE_EXTENDED_DATA;
            default:
                throw new GeException("Unhandled element type: " + elementType);
        }
    }
}
