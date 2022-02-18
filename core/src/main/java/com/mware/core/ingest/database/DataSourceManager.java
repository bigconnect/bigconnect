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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.ingest.database.model.ClientApiDataSourcePreview;
import com.mware.core.ingest.structured.util.ProgressReporter;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
import java.util.Properties;

@Singleton
public class DataSourceManager {
    BcLogger LOGGER = BcLoggerFactory.getLogger(DataSourceManager.class);

    @Inject
    public DataSourceManager() {
    }

    public void startImport(ClientApiDataSource params, ProgressReporter progressReporter) throws Exception {
        try {
            DataSourceImportJob importJob = InjectHelper.getInstance(DataSourceImportJob.class);
            importJob.prepare(params, progressReporter);
            importJob.run();
        } catch (Exception ex) {
            LOGGER.error("Failed running DataSourceImportJob", ex);
            ex.printStackTrace();
        }
    }

    public ClientApiDataSourcePreview generatePreview(DataConnection dataConnection, String sqlSelect) throws BcException {
        try (Connection sqlConn = getSqlConnection(dataConnection)) {
            sqlConn.setAutoCommit(false);

            Statement stmt = sqlConn.createStatement();
            stmt.setMaxRows(20);

            ResultSet rs = stmt.executeQuery(sqlSelect);
            ResultSetMetaData rsm = rs.getMetaData();

            ClientApiDataSourcePreview preview = new ClientApiDataSourcePreview();

            for (int i = 1; i <= rsm.getColumnCount(); i++) {
                ClientApiDataSourcePreview.Column col = new ClientApiDataSourcePreview.Column();

                col.setName(rsm.getColumnName(i));
                col.setTypeName(rsm.getColumnTypeName(i));
                col.setTable(rsm.getTableName(i));
                preview.getColumns().add(col);
            }

            int rowCount = 0;
            while(rs.next()) {
                ClientApiDataSourcePreview.Row row = new ClientApiDataSourcePreview.Row();
                for (int i = 1; i <= rsm.getColumnCount(); i++) {
                    row.getColumns().add(rs.getString(i));
                }
                preview.getRows().add(row);
                if(++rowCount > 20)
                    break;
            }

            return preview;
        } catch (SQLException e) {
            throw new BcException("SQL Exception", e);
        }
    }

    public Connection getSqlConnection(DataConnection dataConnection) throws BcException {
        try {
            Class driverClass = Class.forName(dataConnection.getDriverClass());
            Driver driver = (Driver) driverClass.newInstance();
            DriverManager.registerDriver(driver);

            if(!StringUtils.isEmpty(dataConnection.getDriverProperties())) {
                Properties props = new Properties();
                props.load(new StringReader(dataConnection.getDriverProperties()));
                props.put("user", dataConnection.getUsername());
                props.put("password", dataConnection.getPassword());

                return DriverManager.getConnection(dataConnection.getJdbcUrl(), props);
            } else {
                return DriverManager.getConnection(
                        dataConnection.getJdbcUrl(),
                        dataConnection.getUsername(),
                        dataConnection.getPassword()
                );
            }
        } catch (ClassNotFoundException e) {
            throw new BcException("Driver "+dataConnection.getDriverClass()+" was not found in classpath", e);
        } catch (IllegalAccessException e) {
            throw new BcException(e.getMessage(), e);
        } catch (InstantiationException e) {
            throw new BcException("Cannot create driver "+dataConnection.getDriverClass(), e);
        } catch (SQLException e) {
            throw new BcException(e.getMessage());
        } catch (IOException e) {
            throw new BcException("Problem reading driver properties", e);
        }
    }
}
