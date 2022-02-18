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
 *
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
package com.mware.core.ingest.database;

import com.mware.core.exception.BcException;
import com.mware.core.ingest.database.model.ClientApiDataConnection;
import com.mware.core.ingest.database.model.ClientApiDataConnections;
import com.mware.core.ingest.database.model.ClientApiDataSource;

import java.time.ZonedDateTime;

public interface DataConnectionRepository {
    public static final String VISIBILITY_STRING = "dataConnection";

    Iterable<DataConnection> getAllDataConnections();
    DataConnection findDcById(String dataConnectionId);
    DataConnection findByName(String dataConnectionName);
    DataConnection createDataConnection(String name, String description, String driverClass, String driverProperties, String jdbcUrl, String username, String password);
    boolean checkDataConnection(DataConnection dataConnection) throws BcException;
    void updateDataConnection(String id, String name, String description, String driverClass, String driverProperties, String jdbcUrl, String username, String password);
    void deleteDataConnection(String id);

    Iterable<DataSource> getDataSources(String dataConnectionId);
    DataSource findDsById(String dataSourceId);
    DataSource createDataSource(ClientApiDataSource data);
    void updateDataSource(ClientApiDataSource data);
    void deleteDataSource(String dataSourceId);
    void setImportRunning(String dataSourceId, boolean running);
    void setLastImportDate(String dataSourceId, ZonedDateTime lastRun);

    DataSourceManager getDataSourceManager();

    ClientApiDataConnections toClientApi(Iterable<DataConnection> dataConnections);
    ClientApiDataConnection toClientApi(DataConnection dataConnection);
    ClientApiDataSource toClientApi(String dataConnectionId, DataSource dataSource);
}
