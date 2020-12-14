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

import com.mware.core.orm.SimpleOrmContext;
import com.mware.core.orm.SimpleOrmSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemorySimpleOrmSession extends SimpleOrmSession {
    private Map<Class, InMemoryOrmTable> tables;

    public InMemorySimpleOrmSession() {
        this(new HashMap<>());
    }

    public InMemorySimpleOrmSession(Map<Class, InMemoryOrmTable> tables) {
        this.tables = tables;
    }

    @Override
    public SimpleOrmContext createContext(String... authorizations) {
        return new InMemorySimpleOrmContext(authorizations);
    }

    @Override
    public String getTablePrefix() {
        return "";
    }

    @Override
    public Iterable<String> getTableList(SimpleOrmContext simpleOrmContext) {
        List<String> tableList = new ArrayList<>();
        for (InMemoryOrmTable table : tables.values()) {
            tableList.add(table.getName());
        }
        return tableList;
    }

    @Override
    public void deleteTable(String tableName, SimpleOrmContext simpleOrmContext) {
        for (Map.Entry<Class, InMemoryOrmTable> tableEntry : tables.entrySet()) {
            if (tableEntry.getValue().getName().equals(tableName)) {
                tables.remove(tableEntry.getKey());
            }
        }
    }

    @Override
    public void clearTable(String table, SimpleOrmContext simpleOrmContext) {
        deleteTable(table, simpleOrmContext);
    }

    @Override
    public <T> Iterable<T> findAll(Class<T> rowClass, SimpleOrmContext context) {
        InMemoryOrmTable<T> table = getTable(rowClass);
        return table.findAll(context);
    }

    @Override
    public <T> Iterable<T> findAllInRange(String startKey, String endKey, Class<T> rowClass, SimpleOrmContext context) {
        InMemoryOrmTable<T> table = getTable(rowClass);
        return table.findAllInRange(context);
    }

    @Override
    public <T> T findById(Class<T> rowClass, String id, SimpleOrmContext context) {
        InMemoryOrmTable<T> table = getTable(rowClass);
        return table.findById(id, context);
    }

    @Override
    public <T> Iterable<T> findByIdStartsWith(Class<T> rowClass, String idPrefix, SimpleOrmContext context) {
        InMemoryOrmTable<T> table = getTable(rowClass);
        return table.findByIdStartsWith(idPrefix, context);
    }

    @Override
    public <T> void save(T obj, String visibility, SimpleOrmContext context) {
        //noinspection unchecked
        InMemoryOrmTable<T> table = (InMemoryOrmTable<T>) getTable(obj.getClass());
        table.save(obj, visibility, context);
    }

    @Override
    public <T> void delete(Class<T> rowClass, String id, SimpleOrmContext context) {
        InMemoryOrmTable table = getTable(rowClass);
        table.delete(id, context);
    }

    protected <T> InMemoryOrmTable<T> getTable(Class<T> rowClass) {
        //noinspection unchecked
        InMemoryOrmTable<T> table = tables.get(rowClass);
        if (table == null) {
            Class c = rowClass.getSuperclass();
            while (c != null && table == null) {
                //noinspection unchecked
                table = tables.get(c);
                c = c.getSuperclass();
            }
        }
        if (table != null) {
            return table;
        }
        table = new InMemoryOrmTable<>(rowClass);
        tables.put(table.getEntityRowClass(), table);
        return table;
    }

    @Override
    public void close() {
    }
}
