/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.mware.ge.cypher.builtin.proc.jdbc;

import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.collection.MapUtil;
import com.mware.ge.cypher.procedure.Description;
import com.mware.ge.cypher.procedure.Name;
import com.mware.ge.cypher.procedure.Procedure;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mware.ge.cypher.builtin.proc.jdbc.JdbcUtil.*;

public class JdbcProcedures {
    private static BcLogger log = BcLoggerFactory.getLogger(JdbcProcedures.class);

    @Procedure(name = "jdbc.driver")
    @Description("jdbc.driver('org.apache.derby.jdbc.EmbeddedDriver') register JDBC driver of source database")
    public void driver(@Name("driverClass") String driverClass) {
        loadDriver(driverClass);
    }

    @Procedure(name = "jdbc.load")
    @Description("jdbc.load('url','table or statement', params, config) YIELD row - load from relational database, from a full table or a sql statement")
    public Stream<RowResult> jdbc(
            @Name("jdbc") String urlOrKey,
            @Name("tableOrSql") String tableOrSelect,
            @Name(value = "params", defaultValue = "[]") List<Object> params,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        return executeQuery(urlOrKey, tableOrSelect, config, params.toArray(new Object[params.size()]));
    }

    @Procedure(name = "jdbc.update")
    @Description("jdbc.update('key or url','statement',[params],config) YIELD row - update relational database, from a SQL statement with optional parameters")
    public Stream<RowResult> jdbcUpdate(
            @Name("jdbc") String urlOrKey,
            @Name("query") String query,
            @Name(value = "params", defaultValue = "[]") List<Object> params,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        log.info(String.format("Executing SQL update: %s", query));
        return executeUpdate(urlOrKey, query, config, params.toArray(new Object[params.size()]));
    }

    private Stream<RowResult> executeQuery(String url, String tableOrSelect, Map<String, Object> config, Object... params) {
        LoadJdbcConfig loadJdbcConfig = new LoadJdbcConfig(config);
        String query = tableOrSelect.contains(" ") ? tableOrSelect : ("SELECT * FROM " + tableOrSelect);
        try {
            Connection connection = getConnection(url, loadJdbcConfig);
            // see https://jdbc.postgresql.org/documentation/91/query.html#query-with-cursors
            connection.setAutoCommit(false);
            try {
                PreparedStatement stmt = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(5000);
                try {
                    for (int i = 0; i < params.length; i++) stmt.setObject(i + 1, params[i]);
                    ResultSet rs = stmt.executeQuery();
                    Iterator<Map<String, Object>> supplier = new ResultSetIterator(log, rs, true, loadJdbcConfig);
                    Spliterator<Map<String, Object>> spliterator = Spliterators.spliteratorUnknownSize(supplier, Spliterator.ORDERED);
                    return StreamSupport.stream(spliterator, false)
                            .map(RowResult::new)
                            .onClose(() -> closeIt(log, stmt, connection));
                } catch (Exception sqle) {
                    closeIt(log, stmt);
                    throw sqle;
                }
            } catch (Exception sqle) {
                closeIt(log, connection);
                throw sqle;
            }
        } catch (Exception e) {
            log.error(String.format("Cannot execute SQL statement `%s`.%nError:%n%s", query, e.getMessage()), e);
            String errorMessage = "Cannot execute SQL statement `%s`.%nError:%n%s";
            if (e.getMessage().contains("No suitable driver"))
                errorMessage = "Cannot execute SQL statement `%s`.%nError:%n%s%n%s";
            throw new RuntimeException(String.format(errorMessage, query, e.getMessage(), "Please download and copy the JDBC driver into $BIGCONNECT_DIR/lib/ext"), e);
        }
    }

    private Stream<RowResult> executeUpdate(String url, String query, Map<String, Object> config, Object...params) {
        LoadJdbcConfig jdbcConfig = new LoadJdbcConfig(config);
        try {
            Connection connection = getConnection(url,jdbcConfig);
            try {
                PreparedStatement stmt = connection.prepareStatement(query,ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                stmt.setFetchSize(5000);
                try {
                    for (int i = 0; i < params.length; i++) stmt.setObject(i + 1, params[i]);
                    int updateCount = stmt.executeUpdate();
                    closeIt(log, stmt, connection);
                    Map<String, Object> result = MapUtil.map("count", updateCount);
                    return Stream.of(result)
                            .map(RowResult::new);
                } catch(Exception sqle) {
                    closeIt(log,stmt);
                    throw sqle;
                }
            } catch(Exception sqle) {
                closeIt(log,connection);
                throw sqle;
            }
        } catch (Exception e) {
            log.error(String.format("Cannot execute SQL statement `%s`.%nError:%n%s", query, e.getMessage()),e);
            String errorMessage = "Cannot execute SQL statement `%s`.%nError:%n%s";
            if(e.getMessage().contains("No suitable driver")) errorMessage="Cannot execute SQL statement `%s`.%nError:%n%s%n%s";
            throw new RuntimeException(String.format(errorMessage, query, e.getMessage(), "Please download and copy the JDBC driver into $NEO4J_HOME/plugins,more details at https://neo4j-contrib.github.io/neo4j-apoc-procedures/#_load_jdbc_resources"), e);
        }
    }

    static void closeIt(BcLogger log, AutoCloseable... closeables) {
        for (AutoCloseable c : closeables) {
            try {
                if (c != null) {
                    c.close();
                }
            } catch (Exception e) {
                log.warn(String.format("Error closing %s: %s", c.getClass().getSimpleName(), c), e);
                // ignore
            }
        }
    }

    private static void loadDriver(@Name("driverClass") String driverClass) {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load driver class " + driverClass + " " + e.getMessage());
        }
    }

    interface FailingSupplier<T> {
        T get() throws Exception;
    }

    public static <T> T ignore(FailingSupplier<T> fun) {
        try {
            return fun.get();
        } catch (Exception e) {
            /*ignore*/
        }
        return null;
    }

    private static class ResultSetIterator implements Iterator<Map<String, Object>> {
        private final BcLogger log;
        private final ResultSet rs;
        private final String[] columns;
        private final boolean closeConnection;
        private Map<String, Object> map;
        private LoadJdbcConfig config;


        public ResultSetIterator(BcLogger log, ResultSet rs, boolean closeConnection, LoadJdbcConfig config) throws SQLException {
            this.config = config;
            this.log = log;
            this.rs = rs;
            this.columns = getMetaData(rs);
            this.closeConnection = closeConnection;
            this.map = get();
        }

        private String[] getMetaData(ResultSet rs) throws SQLException {
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            String[] columns = new String[cols + 1];
            for (int col = 1; col <= cols; col++) {
                columns[col] = meta.getColumnLabel(col);
            }
            return columns;
        }

        @Override
        public boolean hasNext() {
            return this.map != null;
        }

        @Override
        public Map<String, Object> next() {
            Map<String, Object> current = this.map;
            this.map = get();
            return current;
        }

        public Map<String, Object> get() {
            try {
                if (handleEndOfResults()) return null;
                Map<String, Object> row = new LinkedHashMap<>(columns.length);
                for (int col = 1; col < columns.length; col++) {
                    row.put(columns[col], convert(rs.getObject(col), rs.getMetaData().getColumnType(col)));
                }
                return row;
            } catch (Exception e) {
                log.error(String.format("Cannot execute read result-set.%nError:%n%s", e.getMessage()), e);
                closeRs();
                throw new RuntimeException("Cannot execute read result-set.", e);
            }
        }

        private Object convert(Object value, int sqlType) {
            if (value == null) return null;
            if (value instanceof UUID || value instanceof BigInteger || value instanceof BigDecimal) {
                return value.toString();
            }
            if (Types.TIME == sqlType) {
                return ((java.sql.Time) value).toLocalTime();
            }
            if (Types.TIME_WITH_TIMEZONE == sqlType) {
                return OffsetTime.parse(value.toString());
            }
            if (Types.TIMESTAMP == sqlType) {
                if (config.getZoneId() != null) {
                    return ((java.sql.Timestamp) value).toInstant()
                            .atZone(config.getZoneId())
                            .toOffsetDateTime();
                } else {
                    return ((java.sql.Timestamp) value).toLocalDateTime();
                }
            }
            if (Types.TIMESTAMP_WITH_TIMEZONE == sqlType) {
                if (config.getZoneId() != null) {
                    return ((java.sql.Timestamp) value).toInstant()
                            .atZone(config.getZoneId())
                            .toOffsetDateTime();
                } else {
                    return OffsetDateTime.parse(value.toString());
                }
            }
            if (Types.DATE == sqlType) {
                return ((java.sql.Date) value).toLocalDate();
            }
            return value;
        }

        private boolean handleEndOfResults() throws SQLException {
            Boolean closed = isRsClosed();
            if (closed != null && closed) {
                return true;
            }
            if (!rs.next()) {
                closeRs();
                return true;
            }
            return false;
        }

        private void closeRs() {
            Boolean closed = isRsClosed();
            if (closed == null || !closed) {
                closeIt(log, ignore(rs::getStatement), closeConnection ? ignore(() -> rs.getStatement().getConnection()) : null);
            }
        }

        private Boolean isRsClosed() {
            try {
                return ignore(rs::isClosed);
            } catch (AbstractMethodError ame) {
                return null;
            }
        }

    }
}
