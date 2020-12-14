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
package com.mware.core.orm.inmemory;

import com.mware.core.orm.ModelMetadata;
import com.mware.core.orm.ModelMetadataBuilder;
import com.mware.core.orm.SimpleOrmContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InMemoryOrmTable<T> {
    protected final ModelMetadata<T> modelMetadata;
    private final List<SimpleOrmItem<T>> data = new ArrayList<>();

    public InMemoryOrmTable(Class<T> rowClass) {
        this.modelMetadata = ModelMetadataBuilder.build(rowClass);
    }

    public Class getEntityRowClass() {
        return this.modelMetadata.getEntityRowClass();
    }

    public String getName() {
        return this.modelMetadata.getTableName();
    }

    public Iterable<T> findAll(final SimpleOrmContext context) {
        final Iterable<SimpleOrmItem<T>> allItemsIterable = findAllItems(context);
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                final Iterator<SimpleOrmItem<T>> it = allItemsIterable.iterator();
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public T next() {
                        return it.next().getObj();
                    }

                    @Override
                    public void remove() {
                        it.remove();
                    }
                };
            }
        };
    }

    public Iterable<T> findAllInRange(SimpleOrmContext context) {
        throw new UnsupportedOperationException("Not implemented");
    }


    public Iterable<SimpleOrmItem<T>> findAllItems(SimpleOrmContext context) {
        // TODO filter by authorizations
        return this.data;
    }

    public T findById(String id, SimpleOrmContext context) {
        SimpleOrmItem<T> item = findItemById(id, context);
        if (item == null) {
            return null;
        }
        return item.getObj();
    }

    protected SimpleOrmItem<T> findItemById(String id, SimpleOrmContext context) {
        for (SimpleOrmItem<T> item : findAllItems(context)) {
            if (this.modelMetadata.getId(item.getObj()).equals(id)) {
                return item;
            }
        }
        return null;
    }

    public Iterable<T> findByIdStartsWith(String idPrefix, SimpleOrmContext context) {
        List<T> results = new ArrayList<>();
        for (T t : findAll(context)) {
            if (this.modelMetadata.getId(t).startsWith(idPrefix)) {
                results.add(t);
            }
        }
        return results;
    }

    public void save(T obj, String visibility, SimpleOrmContext context) {
        String id = this.modelMetadata.getId(obj);
        delete(id, context);
        this.data.add(new SimpleOrmItem(id, obj, visibility));
    }

    public void delete(String id, SimpleOrmContext context) {
        SimpleOrmItem item = findItemById(id, context);
        if (item == null) {
            return;
        }
        this.data.remove(item);
    }
}
