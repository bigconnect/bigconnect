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
package com.mware.ge.accumulo;

import com.mware.ge.GeException;
import com.mware.ge.accumulo.util.DataInDataTableStreamingPropertyValueStorageStrategy;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.commons.lang3.StringUtils;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mware.ge.base.GraphTestSetup.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class AccumuloResource extends ExternalResource {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(AccumuloResource.class);

    private static final String ACCUMULO_USERNAME = "root";
    private static final String ACCUMULO_PASSWORD = "test";

    private File tempDir;
    private MiniAccumuloCluster accumulo;

    private Map extraConfig = null;
    private String instanceName = null;

    static Connector connector;

    public AccumuloResource() {
    }

    public AccumuloResource(Map extraConfig) {
        this.extraConfig = extraConfig;
    }

    public AccumuloResource(String instanceName, Map extraConfig) {
        this.instanceName = instanceName;
        this.extraConfig = extraConfig;
    }

    @Override
    protected void before() throws Throwable {
//        System.setProperty("REMOTE_ACC_ZOOKEEPERS", "localhost");
//        System.setProperty("REMOTE_ACC_INSTANCE", "accumulo");
//        System.setProperty("REMOTE_ACC_PASSWORD", "secret");

        ensureAccumuloIsStarted();
        super.before();
    }

    @Override
    protected void after() {
        try {
            stop();
        } catch (Exception e) {
            LOGGER.info("Unable to shut down mini accumulo cluster", e);
        }
        super.after();
    }

    public void resetAutorizations() throws Exception {
        Connector connector = getConnector();
        connector.securityOperations().changeUserAuthorizations(
                AccumuloGraphConfiguration.DEFAULT_ACCUMULO_USERNAME,
                new Authorizations(
                        VISIBILITY_A_STRING,
                        VISIBILITY_B_STRING,
                        VISIBILITY_C_STRING,
                        VISIBILITY_MIXED_CASE_STRING
                )
        );
    }

    public void addAuthorizations(AccumuloGraph graph, String... authorizations) {
        try {
            String principal = graph.getConnector().whoami();
            Authorizations currentAuthorizations = graph.getConnector().securityOperations().getUserAuthorizations(principal);

            List<byte[]> newAuthorizationsArray = new ArrayList<>();
            for (byte[] currentAuth : currentAuthorizations) {
                newAuthorizationsArray.add(currentAuth);
            }

            for (String authorization : authorizations) {
                if (!currentAuthorizations.contains(authorization)) {
                    newAuthorizationsArray.add(authorization.getBytes(UTF_8));
                }
            }

            Authorizations newAuthorizations = new Authorizations(newAuthorizationsArray);
            graph.getConnector().securityOperations().changeUserAuthorizations(principal, newAuthorizations);
        } catch (Exception ex) {
            throw new GeException("could not add authorizations", ex);
        }
    }

    public MiniAccumuloCluster getAccumulo() {
        return accumulo;
    }

    @SuppressWarnings("unchecked")
    public Map createConfig() {
        Map configMap = new HashMap();
        if (shouldUseRemoteAccumulo()) {
            configMap.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, System.getProperty("REMOTE_ACC_ZOOKEEPERS"));
            configMap.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, System.getProperty("REMOTE_ACC_INSTANCE", "accumulo"));
            configMap.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, System.getProperty("REMOTE_ACC_PASSWORD", ACCUMULO_PASSWORD));

        } else {
            configMap.put(AccumuloGraphConfiguration.ZOOKEEPER_SERVERS, accumulo.getZooKeepers());
            configMap.put(AccumuloGraphConfiguration.ACCUMULO_INSTANCE_NAME, accumulo.getInstanceName());
            configMap.put(AccumuloGraphConfiguration.ACCUMULO_PASSWORD, ACCUMULO_PASSWORD);
        }
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_USERNAME, ACCUMULO_USERNAME);
        configMap.put(AccumuloGraphConfiguration.AUTO_FLUSH, AccumuloGraphConfiguration.DEFAULT_AUTO_FLUSH);
        configMap.put(AccumuloGraphConfiguration.MAX_STREAMING_PROPERTY_VALUE_TABLE_DATA_SIZE, LARGE_PROPERTY_VALUE_SIZE - 1);
        configMap.put(AccumuloGraphConfiguration.DATA_DIR, "/tmp/");
        configMap.put(AccumuloGraphConfiguration.ACCUMULO_MAX_VERSIONS, 1);
        configMap.put(AccumuloGraphConfiguration.HISTORY_IN_SEPARATE_TABLE, false);
        configMap.put(AccumuloGraphConfiguration.COMPRESS_ITERATOR_TRANSFERS, false);
        configMap.put(AccumuloGraphConfiguration.STREAMING_PROPERTY_VALUE_STORAGE_STRATEGY_PREFIX, DataInDataTableStreamingPropertyValueStorageStrategy.class.getName());

        if (extraConfig != null) {
            configMap.putAll(extraConfig);
        }

        return configMap;
    }

    public Connector createConnector() throws AccumuloSecurityException, AccumuloException {
        return new AccumuloGraphConfiguration(createConfig()).createConnector();
    }

    public void ensureAccumuloIsStarted() {
        if (shouldUseRemoteAccumulo())
            return;

        try {
            start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Accumulo mini cluster", e);
        }
    }

    public void stop() throws IOException, InterruptedException {
        if (shouldUseRemoteAccumulo())
            return;

        if (accumulo != null) {
            LOGGER.info("Stopping accumulo");
            accumulo.stop();
            accumulo = null;
        }
        tempDir.delete();
    }

    public void start() throws IOException, InterruptedException {
        if (shouldUseRemoteAccumulo())
            return;

        if (accumulo != null) {
            return;
        }

        LOGGER.info("Starting accumulo");

        tempDir = File.createTempFile("accumulo-temp", Long.toString(System.nanoTime()));
        tempDir.delete();
        tempDir.mkdir();
        LOGGER.info("writing to: %s", tempDir);

        MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(tempDir, ACCUMULO_PASSWORD);
        miniAccumuloConfig.setMemory(ServerType.TABLET_SERVER, 2, MemoryUnit.GIGABYTE);
        miniAccumuloConfig.setNumTservers(1);
        miniAccumuloConfig.setJDWPEnabled(true);
        miniAccumuloConfig.setZooKeeperStartupTime(60000);

        if (instanceName != null) {
            miniAccumuloConfig.setInstanceName(instanceName);
        }
        accumulo = new MiniAccumuloCluster(miniAccumuloConfig);
        accumulo.start();
        accumulo.getDebugPorts().forEach(serverTypeIntegerPair -> {
            LOGGER.info("##### Debug port: "+serverTypeIntegerPair.getFirst().prettyPrint()+"="+serverTypeIntegerPair.getSecond());
        });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    AccumuloResource.this.stop();
                } catch (Exception e) {
                    System.out.println("Failed to stop Accumulo test cluster");
                }
            }
        });
    }

    private boolean shouldUseRemoteAccumulo() {
        return StringUtils.isNotEmpty(getRemoteZookeepers());
    }

    private String getRemoteZookeepers() {
        return System.getProperty("REMOTE_ACC_ZOOKEEPERS");
    }

    Connector getConnector() throws Exception {
        if (connector == null) {
            connector = createConnector();
        }

        return connector;
    }
}
