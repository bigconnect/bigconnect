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

import com.mware.ge.GeObjectId;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

import java.util.ArrayList;
import java.util.List;

public abstract class BulkItem<T extends Item> {
    private final String indexName;
    private final String type;
    private final String documentId;
    private final GeObjectId geObjectId;
    private final List<T> items = new ArrayList<>();

    public BulkItem(
            String indexName,
            String type,
            String documentId,
            GeObjectId geObjectId
    ) {
        this.indexName = indexName;
        this.type = type;
        this.documentId = documentId;
        this.geObjectId = geObjectId;
    }

    public void add(T item) {
        items.add(item);
    }

    public abstract void addToBulkRequest(Client client, BulkRequestBuilder bulkRequestBuilder);

    public void complete() {
        for (T item : items) {
            item.complete();
        }
    }

    public void completeExceptionally(Exception exception) {
        for (T item : items) {
            item.completeExceptionally(exception);
        }
    }

    public void incrementFailCount() {
        for (T item : items) {
            item.incrementFailCount();
        }
    }

    public void updateLastTriedTime() {
        for (T item : items) {
            item.updateLastTriedTime();
        }
    }

    public String getDocumentId() {
        return documentId;
    }

    public int getFailCount() {
        return items.stream()
                .map(Item::getFailCount)
                .min(Integer::compareTo)
                .orElse(0);
    }

    public String getIndexName() {
        return indexName;
    }

    public List<T> getItems() {
        return items;
    }

    public abstract int getSize();

    public String getType() {
        return type;
    }

    public GeObjectId getGeObjectId() {
        return geObjectId;
    }
}
