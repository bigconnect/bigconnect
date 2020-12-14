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
package com.mware.ge.elasticsearch5.bulk;

import com.mware.ge.ElementLocation;
import com.mware.ge.GeObjectId;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;

import java.util.Collection;
import java.util.Map;

import static com.mware.ge.elasticsearch5.bulk.BulkUtils.*;

public class UpdateItem extends Item {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(UpdateItem.class);
    private final ElementLocation sourceElementLocation;
    private final Map<String, String> source;
    private final Map<String, Object> fieldsToSet;
    private final Collection<String> fieldsToRemove;
    private final Map<String, String> fieldsToRename;
    private final boolean existingElement;
    private final int size;

    public UpdateItem(
            String indexName,
            String type,
            String docId,
            GeObjectId geObjectId,
            ElementLocation sourceElementLocation,
            Map<String, String> source,
            Map<String, Object> fieldsToSet,
            Collection<String> fieldsToRemove,
            Map<String, String> fieldsToRename,
            boolean existingElement
    ) {
        super(indexName, type, docId, geObjectId);
        this.sourceElementLocation = sourceElementLocation;
        this.source = source;
        this.fieldsToSet = fieldsToSet;
        this.fieldsToRemove = fieldsToRemove;
        this.fieldsToRename = fieldsToRename;
        this.existingElement = existingElement;

        this.size = getIndexName().length()
                + type.length()
                + docId.length()
                + calculateSizeOfId(geObjectId)
                + calculateSizeOfMap(source)
                + calculateSizeOfMap(fieldsToSet)
                + calculateSizeOfCollection(fieldsToRemove)
                + calculateSizeOfMap(fieldsToRename);
    }

   public Collection<String> getFieldsToRemove() {
        return fieldsToRemove;
    }

    public Map<String, String> getFieldsToRename() {
        return fieldsToRename;
    }

    public Map<String, Object> getFieldsToSet() {
        return fieldsToSet;
    }

    @Override
    public int getSize() {
        return size;
    }

    public Map<String, String> getSource() {
        return source;
    }

    public ElementLocation getSourceElementLocation() {
        return sourceElementLocation;
    }

    public boolean isExistingElement() {
        return existingElement;
    }
}
