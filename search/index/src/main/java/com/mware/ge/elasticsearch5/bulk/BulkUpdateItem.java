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

import com.google.common.collect.ImmutableMap;
import com.mware.ge.ElementLocation;
import com.mware.ge.GeException;
import com.mware.ge.GeObjectId;
import com.mware.ge.elasticsearch5.Elasticsearch5SearchIndex;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.util.*;
import java.util.stream.Collectors;

import static com.mware.ge.elasticsearch5.bulk.BulkUtils.*;

public class BulkUpdateItem extends BulkItem<UpdateItem> {
    private final ElementLocation sourceElementLocation;
    private final Map<String, String> source = new HashMap<>();
    private final Map<String, Set<Object>> fieldsToSet = new HashMap<>();
    private final Set<String> fieldsToRemove = new HashSet<>();
    private final Map<String, String> fieldsToRename = new HashMap<>();
    private boolean updateOnly = true;
    private Integer size;

    public BulkUpdateItem(
            String indexName,
            String type,
            String documentId,
            GeObjectId geObjectId,
            ElementLocation sourceElementLocation
    ) {
        super(indexName, type, documentId, geObjectId);
        this.sourceElementLocation = sourceElementLocation;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void add(UpdateItem item) {
        super.add(item);
        size = null;

        for (Map.Entry<String, Object> itemEntry : item.getFieldsToSet().entrySet()) {
            Object itemValue = itemEntry.getValue();
            fieldsToSet.compute(itemEntry.getKey(), (key, existingValue) -> {
                if (existingValue == null) {
                    if (itemValue instanceof Collection) {
                        return new HashSet<>((Collection<?>) itemValue);
                    } else {
                        Set newValue = new HashSet<>();
                        newValue.add(itemValue);
                        return newValue;
                    }
                } else {
                    if (itemValue instanceof Collection) {
                        existingValue.addAll((Collection) itemValue);
                    } else {
                        existingValue.add(itemValue);
                    }
                    return existingValue;
                }
            });
        }

        for (Map.Entry<String, String> itemEntry : item.getFieldsToRename().entrySet()) {
            String itemValue = itemEntry.getValue();
            fieldsToRename.compute(itemEntry.getKey(), (key, existingValue) -> {
                if (existingValue == null) {
                    return itemValue;
                } else if (existingValue.equals(itemValue)) {
                    return itemValue;
                } else {
                    throw new GeException("Changing the same property to two different visibilities in the same batch is not allowed: " + itemEntry.getKey());
                }
            });
        }

        source.putAll(item.getSource());
        fieldsToRemove.addAll(item.getFieldsToRemove());
        if (!item.isExistingElement()) {
            updateOnly = false;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void addToBulkRequest(Client client, BulkRequestBuilder bulkRequestBuilder) {
        UpdateRequestBuilder updateRequestBuilder = client
                .prepareUpdate(getIndexName(), getType(), getDocumentId());
        if (!updateOnly) {
            updateRequestBuilder = updateRequestBuilder
                    .setScriptedUpsert(true)
                    .setUpsert((Map<String, Object>) (Map) source);
        }
        UpdateRequest updateRequest = updateRequestBuilder
                .setScript(new Script(
                        ScriptType.STORED,
                        null,
                        "updateFieldsOnDocumentScript",
                        ImmutableMap.of(
                                "fieldsToSet", fieldsToSet.entrySet().stream()
                                        .collect(Collectors.toMap(
                                                Map.Entry::getKey,
                                                entry -> new ArrayList<>(entry.getValue())
                                        )),
                                "fieldsToRemove", new ArrayList<>(fieldsToRemove),
                                "fieldsToRename", fieldsToRename
                        )
                ))
                .setRetryOnConflict(Elasticsearch5SearchIndex.MAX_RETRIES)
                .request();
        bulkRequestBuilder.add(updateRequest);
    }

    @Override
    public int getSize() {
        if (size == null) {
            size = getIndexName().length()
                    + getType().length()
                    + getDocumentId().length()
                    + calculateSizeOfId(getGeObjectId())
                    + calculateSizeOfMap(source)
                    + calculateSizeOfMap(fieldsToSet)
                    + calculateSizeOfCollection(fieldsToRemove)
                    + calculateSizeOfMap(fieldsToRename);
        }
        return size;
    }

    public ElementLocation getSourceElementLocation() {
        return sourceElementLocation;
    }
}
