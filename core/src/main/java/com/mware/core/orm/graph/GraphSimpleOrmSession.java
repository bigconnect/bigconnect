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
package com.mware.core.orm.graph;

import com.google.inject.Inject;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.orm.ModelMetadata;
import com.mware.core.orm.ModelMetadataBuilder;
import com.mware.core.orm.SimpleOrmContext;
import com.mware.core.orm.SimpleOrmSession;
import com.mware.ge.*;
import com.mware.ge.search.IndexHint;
import com.mware.ge.serializer.GeSerializer;
import com.mware.ge.util.ConvertingIterable;
import com.mware.ge.util.StreamUtils;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.Values;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class GraphSimpleOrmSession extends SimpleOrmSession {
    private static final String ORM_VERTEX_ID = "__ORM__";
    private static final String EXTDATA_ROW_COLUMN = "__VALUE__";
    private static final Visibility ORM_VERTEX_VISIBILITY = new Visibility("__ORM__");

    private final GraphBaseWithSearchIndex graph;
    private final GeSerializer serializer;
    private Map<Class, OrmTable> cache = new ConcurrentHashMap<>();

    @Inject
    public GraphSimpleOrmSession(Graph graph) {
        this.graph = (GraphBaseWithSearchIndex) graph;
        this.serializer = this.graph.getConfiguration().createSerializer(graph);
        graph.createAuthorizations(ORM_VERTEX_VISIBILITY.getVisibilityString());
    }

    @Override
    public SimpleOrmContext createContext(String... authorizations) {
        return new GraphSimpleOrmContext(authorizations);
    }

    @Override
    public String getTablePrefix() {
        return "";
    }

    @Override
    public Iterable<String> getTableList(SimpleOrmContext simpleOrmContext) {
        Vertex orm = getOrmVertex(simpleOrmContext.getAuthorizations());
        return orm.getExtendedDataTableNames();
    }

    @Override
    public void deleteTable(String table, SimpleOrmContext context) {
        Vertex orm = getOrmVertex(context.getAuthorizations());
        // do nohing
        orm.prepareMutation()
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .deleteExtendedDataTable(table)
                .save(context.getAuthorizations());
    }

    @Override
    public void clearTable(String table, SimpleOrmContext context) {
        deleteTable(table, context);
    }

    @Override
    public <T> Iterable<T> findAll(Class<T> rowClass, SimpleOrmContext context) {
        Vertex orm = getOrmVertex(context.getAuthorizations());
        OrmTable<T> table = getTable(rowClass);

        return new ConvertingIterable<ExtendedDataRow, T>(orm.getExtendedData(table.getName())) {
            @Override
            protected T convert(ExtendedDataRow row) {
                return realValue(row);
            }
        };
    }

    private <T> T realValue(ExtendedDataRow row) {
        if (row != null) {
            ByteArray value = (ByteArray) row.getPropertyValue(EXTDATA_ROW_COLUMN);
            if (value == null)
                return null;

            return serializer.bytesToObject(value.asObjectCopy());
        } else
            return null;
    }

    @Override
    public <T> Iterable<T> findAllInRange(String startKey, String endKey, Class<T> rowClass, SimpleOrmContext context) {
        throw new GeNotSupportedException("Not implemented");
    }

    @Override
    public <T> T findById(Class<T> rowClass, String id, SimpleOrmContext context) {
        OrmTable<T> table = getTable(rowClass);

        ExtendedDataRow row =
                graph.getExtendedData(new ExtendedDataRowId(ElementType.VERTEX, ORM_VERTEX_ID, table.getName(), id), context.getAuthorizations());

        return realValue(row);
    }

    @Override
    public <T> Iterable<T> findByIdStartsWith(Class<T> rowClass, String idPrefix, SimpleOrmContext context) {
        OrmTable<T> table = getTable(rowClass);
        return () -> StreamUtils.stream(findAll(rowClass, context))
                .filter(t -> table.getId(t).startsWith(idPrefix))
                .iterator();
    }

    @Override
    public <T> void save(T obj, String visibility, SimpleOrmContext context) {
        Vertex orm = getOrmVertex(context.getAuthorizations());
        OrmTable<T> table = (OrmTable<T>) getTable(obj.getClass());
        String rowId = table.getId(obj);
        Visibility rowVisibility = new Visibility(visibility);

        byte[] val = serializer.objectToBytes(obj);

        orm.prepareMutation()
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .addExtendedData(table.getName(), rowId, EXTDATA_ROW_COLUMN, Values.byteArray(val), rowVisibility)
                .save(context.getAuthorizations());

    }

    @Override
    public <T> void delete(Class<T> rowClass, String id, SimpleOrmContext context) {
        Vertex orm = getOrmVertex(context.getAuthorizations());
        OrmTable<T> table = getTable(rowClass);
        orm.prepareMutation()
                .setIndexHint(IndexHint.DO_NOT_INDEX)
                .deleteExtendedData(table.getName(), id)
                .save(context.getAuthorizations());
    }

    @Override
    public void close() {
        // do nothing
    }

    private Vertex getOrmVertex(Authorizations authorizations) {
        Vertex v = graph.getVertex(ORM_VERTEX_ID, authorizations);
        if (v == null) {
            v = graph.prepareVertex(ORM_VERTEX_ID, ORM_VERTEX_VISIBILITY, SchemaConstants.CONCEPT_TYPE_THING)
                    .save(authorizations);
        }
        return v;
    }

    protected <T> OrmTable<T> getTable(Class<T> rowClass) {
        //noinspection unchecked
        OrmTable<T> table = cache.get(rowClass);
        if (table == null) {
            Class c = rowClass.getSuperclass();
            while (c != null && table == null) {
                //noinspection unchecked
                table = cache.get(c);
                c = c.getSuperclass();
            }
        }
        if (table != null) {
            return table;
        }
        table = new OrmTable<>(rowClass);
        cache.put(table.getEntityRowClass(), table);
        return table;
    }


    public static class OrmTable<T> {
        protected final ModelMetadata<T> modelMetadata;

        public OrmTable(Class<T> rowClass) {
            this.modelMetadata = ModelMetadataBuilder.build(rowClass);
        }

        public Class getEntityRowClass() {
            return this.modelMetadata.getEntityRowClass();
        }

        public String getName() {
            return this.modelMetadata.getTableName();
        }

        public Set<ModelMetadata.Field> getColumns() {
            return modelMetadata.getFields();
        }

        public String getId(T obj) {
            return modelMetadata.getId(obj);
        }
    }
}
