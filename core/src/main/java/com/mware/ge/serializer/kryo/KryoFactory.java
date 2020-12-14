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
package com.mware.ge.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializerConfig;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.mware.ge.EdgesSummary;
import com.mware.ge.GeException;
import com.mware.ge.store.StorableEdgeInfo;
import com.mware.ge.values.storable.StreamingPropertyValueRef;
import com.mware.ge.serializer.kryo.customserializers.EdgesSummarySerializer;
import com.mware.ge.serializer.kryo.customserializers.GuavaSerializers;
import com.mware.ge.type.*;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.util.Date;
import java.util.HashMap;

public class KryoFactory {
    public Kryo createKryo() {
        Kryo kryo = new Kryo(new DefaultClassResolver(), new MapReferenceResolver() {
            @Override
            public boolean useReferences(Class type) {
                // avoid calling System.identityHashCode
                if (type == String.class || type == Date.class) {
                    return false;
                }
                return super.useReferences(type);
            }
        });
        registerClasses(kryo);

        kryo.setAutoReset(true);

        // needed for serializing anonymous inner classes so widely used as custom iterables
        FieldSerializerConfig fieldSerializerConfig = kryo.getFieldSerializerConfig();
        fieldSerializerConfig.setIgnoreSyntheticFields(false);

        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        return kryo;
    }

    private void registerClasses(Kryo kryo) {
        kryo.register(StorableEdgeInfo.class, 1000);
        kryo.register(HashMap.class, 1002);
        kryo.register(StreamingPropertyValueRef.class, 1003);
        registerAccumuloClasses(kryo);
        kryo.register(EdgesSummary.class, new EdgesSummarySerializer(), 1013);
        registerGuavaImmutableClasses(kryo);
    }

    private void registerGuavaImmutableClasses(Kryo kryo) {
        GuavaSerializers.register(kryo);
    }

    private void registerAccumuloClasses(Kryo kryo) {
        try {
            Class.forName("com.mware.ge.accumulo.AccumuloGraph");
        } catch (ClassNotFoundException e) {
            // this is ok and expected if Accumulo is not in the classpath
            return;
        }
        try {
            kryo.register(Class.forName("com.mware.ge.accumulo.StreamingPropertyValueTableRef"), 1004);
            kryo.register(Class.forName("com.mware.ge.accumulo.StreamingPropertyValueHdfsRef"), 1005);
        } catch (ClassNotFoundException ex) {
            throw new GeException("Could not find accumulo classes to serialize", ex);
        }
    }
}
