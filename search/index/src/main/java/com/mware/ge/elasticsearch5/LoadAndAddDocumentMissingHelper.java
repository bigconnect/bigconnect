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
import com.mware.ge.elasticsearch5.bulk.BulkItem;
import com.mware.ge.elasticsearch5.bulk.BulkUpdateItem;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.util.Collections;

public class LoadAndAddDocumentMissingHelper implements Elasticsearch5ExceptionHandler {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(LoadAndAddDocumentMissingHelper.class);

    public static void handleDocumentMissingException(
            Graph graph,
            Elasticsearch5SearchIndex searchIndex,
            BulkItem bulkItem,
            Authorizations authorizations
    ) {
        LOGGER.info("handleDocumentMissingException (bulkItem: %s)", bulkItem);
        if (bulkItem instanceof BulkUpdateItem) {
            BulkUpdateItem updateBulkItem = (BulkUpdateItem) bulkItem;
            GeObjectId GeObjectId = updateBulkItem.getGeObjectId();
            if (GeObjectId instanceof ExtendedDataRowId) {
                handleExtendedDataRow(graph, searchIndex, updateBulkItem, authorizations);
            } else if (GeObjectId instanceof ElementId) {
                handleElement(graph, searchIndex, updateBulkItem, authorizations);
            } else {
                throw new GeException("unhandled GeObjectId: " + GeObjectId.getClass().getName());
            }
        } else {
            throw new GeException("unhandled bulk item type: " + bulkItem.getClass().getName());
        }
    }

    protected static void handleElement(
            Graph graph,
            Elasticsearch5SearchIndex searchIndex,
            BulkUpdateItem bulkItem,
            Authorizations authorizations
    ) {
        ElementId elementId = (ElementId) bulkItem.getGeObjectId();
        Element element;
        switch (elementId.getElementType()) {
            case VERTEX:
                element = graph.getVertex(elementId.getId(), authorizations);
                break;
            case EDGE:
                element = graph.getEdge(elementId.getId(), authorizations);
                break;
            default:
                throw new GeException("Invalid element type: " + elementId.getElementType());
        }
        if (element == null) {
            throw new GeException("Could not find element: " + elementId.getId());
        }
        searchIndex.addElement(
                graph,
                element,
                null
        );
    }

    protected static void handleExtendedDataRow(
            Graph graph,
            Elasticsearch5SearchIndex searchIndex,
            BulkUpdateItem bulkItem,
            Authorizations authorizations
    ) {
        ExtendedDataRowId extendedDataRowId = (ExtendedDataRowId) bulkItem.getGeObjectId();
        ExtendedDataRowId id = new ExtendedDataRowId(
                extendedDataRowId.getElementType(),
                extendedDataRowId.getElementId(),
                extendedDataRowId.getTableName(),
                extendedDataRowId.getRowId()
        );
        ExtendedDataRow row = graph.getExtendedData(id, authorizations);
        searchIndex.addExtendedData(
                graph,
                bulkItem.getSourceElementLocation(),
                Collections.singletonList(row),
                authorizations
        );
    }
}
