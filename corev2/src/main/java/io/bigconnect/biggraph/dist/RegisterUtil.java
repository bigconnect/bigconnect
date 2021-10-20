/*
 * Copyright 2021 BigConnect Authors
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.bigconnect.biggraph.dist;

import io.bigconnect.biggraph.BigGraphException;
import io.bigconnect.biggraph.backend.serializer.SerializerFactory;
import io.bigconnect.biggraph.backend.store.BackendProviderFactory;
import io.bigconnect.biggraph.config.CoreOptions;
import io.bigconnect.biggraph.config.BigConfig;
import io.bigconnect.biggraph.config.OptionSpace;
import io.bigconnect.biggraph.plugin.BigGraphPlugin;
import io.bigconnect.biggraph.util.E;
import io.bigconnect.biggraph.util.Log;
import io.bigconnect.biggraph.util.VersionUtil;
import io.bigconnect.biggraph.version.CoreVersion;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;

import java.io.InputStream;
import java.util.List;
import java.util.ServiceLoader;

public class RegisterUtil {

    private static final Logger LOG = Log.logger(RegisterUtil.class);

    static {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("dist", DistOptions.instance());
    }

    public static void registerBackends() {
        String confFile = "/backend.properties";
        InputStream input = RegisterUtil.class
                                        .getResourceAsStream(confFile);
        E.checkState(input != null,
                     "Can't read file '%s' as stream", confFile);

        PropertiesConfiguration props = new PropertiesConfiguration();
        props.setDelimiterParsingDisabled(true);
        try {
            props.load(input);
        } catch (ConfigurationException e) {
            throw new BigGraphException("Can't load config file: %s", e, confFile);
        }

        BigConfig config = new BigConfig(props);
        List<String> backends = config.get(DistOptions.BACKENDS);
        for (String backend : backends) {
            registerBackend(backend);
        }
    }

    private static void registerBackend(String backend) {
        switch (backend) {
            case "cassandra":
                registerCassandra();
                break;
            case "scylladb":
                registerScyllaDB();
                break;
            case "hbase":
                registerHBase();
                break;
            case "rocksdb":
                registerRocksDB();
                break;
            case "mysql":
                registerMysql();
                break;
            case "palo":
                registerPalo();
                break;
            case "postgresql":
                registerPostgresql();
                break;
            default:
                throw new BigGraphException("Unsupported backend type '%s'",
                                        backend);
        }
    }

    public static void registerCassandra() {
        // Register config
        OptionSpace.register("cassandra",
                "io.bigconnect.biggraph.backend.store.cassandra.CassandraOptions");
        // Register serializer
        SerializerFactory.register("cassandra",
                "io.bigconnect.biggraph.backend.store.cassandra.CassandraSerializer");
        // Register backend
        BackendProviderFactory.register("cassandra",
                "io.bigconnect.biggraph.backend.store.cassandra.CassandraStoreProvider");
    }

    public static void registerScyllaDB() {
        // Register config
        OptionSpace.register("scylladb",
                "io.bigconnect.biggraph.backend.store.cassandra.CassandraOptions");
        // Register serializer
        SerializerFactory.register("scylladb",
                "io.bigconnect.biggraph.backend.store.cassandra.CassandraSerializer");
        // Register backend
        BackendProviderFactory.register("scylladb",
                "io.bigconnect.biggraph.backend.store.scylladb.ScyllaDBStoreProvider");
    }

    public static void registerHBase() {
        // Register config
        OptionSpace.register("hbase",
                "io.bigconnect.biggraph.backend.store.hbase.HbaseOptions");
        // Register serializer
        SerializerFactory.register("hbase",
                "io.bigconnect.biggraph.backend.store.hbase.HbaseSerializer");
        // Register backend
        BackendProviderFactory.register("hbase",
                "io.bigconnect.biggraph.backend.store.hbase.HbaseStoreProvider");
    }

    public static void registerRocksDB() {
        // Register config
        OptionSpace.register("rocksdb",
                "io.bigconnect.biggraph.backend.store.rocksdb.RocksDBOptions");
        // Register backend
        BackendProviderFactory.register("rocksdb",
                "io.bigconnect.biggraph.backend.store.rocksdb.RocksDBStoreProvider");
        BackendProviderFactory.register("rocksdbsst",
                "io.bigconnect.biggraph.backend.store.rocksdbsst.RocksDBSstStoreProvider");
    }

    public static void registerMysql() {
        // Register config
        OptionSpace.register("mysql",
                "io.bigconnect.biggraph.backend.store.mysql.MysqlOptions");
        // Register serializer
        SerializerFactory.register("mysql",
                "io.bigconnect.biggraph.backend.store.mysql.MysqlSerializer");
        // Register backend
        BackendProviderFactory.register("mysql",
                "io.bigconnect.biggraph.backend.store.mysql.MysqlStoreProvider");
    }

    public static void registerPalo() {
        // Register config
        OptionSpace.register("palo",
                "io.bigconnect.biggraph.backend.store.palo.PaloOptions");
        // Register serializer
        SerializerFactory.register("palo",
                "io.bigconnect.biggraph.backend.store.palo.PaloSerializer");
        // Register backend
        BackendProviderFactory.register("palo",
                "io.bigconnect.biggraph.backend.store.palo.PaloStoreProvider");
    }

    public static void registerPostgresql() {
        // Register config
        OptionSpace.register("postgresql",
                "io.bigconnect.biggraph.backend.store.postgresql.PostgresqlOptions");
        // Register serializer
        SerializerFactory.register("postgresql",
                "io.bigconnect.biggraph.backend.store.postgresql.PostgresqlSerializer");
        // Register backend
        BackendProviderFactory.register("postgresql",
                "io.bigconnect.biggraph.backend.store.postgresql.PostgresqlStoreProvider");
    }

    public static void registerServer() {
        // Register ServerOptions (rest-server)
        OptionSpace.register("server", "io.bigconnect.biggraph.config.ServerOptions");
        // Register RpcOptions (rpc-server)
        OptionSpace.register("rpc", "io.bigconnect.biggraph.config.RpcOptions");
        // Register AuthOptions (auth-server)
        OptionSpace.register("auth", "io.bigconnect.biggraph.config.AuthOptions");
    }

    /**
     * Scan the jars in plugins directory and load them
     */
    public static void registerPlugins() {
        ServiceLoader<BigGraphPlugin> plugins = ServiceLoader.load(
                                                 BigGraphPlugin.class);
        for (BigGraphPlugin plugin : plugins) {
            LOG.info("Loading plugin {}({})",
                     plugin.name(), plugin.getClass().getCanonicalName());
            String minVersion = plugin.supportsMinVersion();
            String maxVersion = plugin.supportsMaxVersion();

            if (!VersionUtil.match(CoreVersion.VERSION, minVersion,
                                   maxVersion)) {
                LOG.warn("Skip loading plugin '{}' due to the version range " +
                         "'[{}, {})' that it's supported doesn't cover " +
                         "current core version '{}'", plugin.name(),
                         minVersion, maxVersion, CoreVersion.VERSION.get());
                continue;
            }
            try {
                plugin.register();
                LOG.info("Loaded plugin '{}'", plugin.name());
            } catch (Exception e) {
                throw new BigGraphException("Failed to load plugin '%s'",
                                        plugin.name(), e);
            }
        }
    }
}
