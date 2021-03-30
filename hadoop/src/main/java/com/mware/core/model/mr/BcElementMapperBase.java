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
package com.mware.core.model.mr;

import com.mware.core.bootstrap.BcBootstrap;
import com.mware.core.bootstrap.config.HdfsConfigurationLoader;
import com.mware.core.config.Configuration;
import com.mware.core.trace.TraceRepository;
import com.mware.ge.id.LongIdGenerator;
import com.mware.ge.store.mutations.StoreMutation;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import com.mware.ge.accumulo.mapreduce.ElementMapper;
import com.mware.ge.id.IdGenerator;
import com.mware.ge.id.UUIDIdGenerator;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.config.ConfigurationLoader;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class BcElementMapperBase<KEYIN, VALUEIN> extends ElementMapper<KEYIN, VALUEIN, Text, StoreMutation> {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(BcElementMapperBase.class);
    private IdGenerator idGenerator = new UUIDIdGenerator();

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        try {
            super.setup(context);
            System.setProperty(ConfigurationLoader.ENV_CONFIGURATION_LOADER, HdfsConfigurationLoader.class.getName());
            Configuration bcConfig = ConfigurationLoader.load();
            InjectHelper.inject(this, BcBootstrap.bootstrapModuleMaker(bcConfig), bcConfig);
            if(bcConfig.getBoolean(com.mware.core.config.Configuration.TRACE_ENABLED, com.mware.core.config.Configuration.DEFAULT_TRACE_ENABLED)) {
                TraceRepository traceRepository = InjectHelper.getInstance(TraceRepository.class);
                traceRepository.enable();
            }
        } catch (Throwable ex) {
            LOGGER.error("Could not setup", ex);
            throw new IOException("Could not setup", ex);
        }
    }

    @Override
    protected void map(KEYIN key, VALUEIN line, Mapper<KEYIN, VALUEIN, Text, StoreMutation>.Context context) {
        try {
            safeMap(key, line, context);
        } catch (Throwable ex) {
            LOGGER.error("failed mapping " + key, ex);
        }
    }

    protected abstract void safeMap(KEYIN key, VALUEIN line, Mapper.Context context) throws Exception;

    @Override
    protected void saveDataMutation(Mapper.Context context, Text dataTableName, StoreMutation m) throws IOException, InterruptedException {
        context.write(getKey(context, dataTableName, m), m);
    }

    @Override
    protected void saveEdgeMutation(Mapper.Context context, Text edgesTableName, StoreMutation m) throws IOException, InterruptedException {
        context.write(getKey(context, edgesTableName, m), m);
    }

    @Override
    protected void saveVertexMutation(Mapper.Context context, Text verticesTableName, StoreMutation m) throws IOException, InterruptedException {
        context.write(getKey(context, verticesTableName, m), m);
    }

    @Override
    protected void saveExtendedDataMutation(Context context, Text extendedDataTableName, StoreMutation m) throws IOException, InterruptedException {
        context.write(getKey(context, extendedDataTableName, m), m);
    }

    protected Text getKey(Mapper.Context context, Text tableName, StoreMutation m) {
        return tableName;
    }

    @Override
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }
}
