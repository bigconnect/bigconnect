/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package com.mware.bigconnect.driver;

import org.junit.Test;

import java.net.URI;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.mware.bigconnect.driver.AuthTokens.basic;
import static com.mware.bigconnect.driver.Logging.slf4j;

public class DriverTest {
    @Test
    public void testHistory() {
        try (Driver driver = getDriver()) {
            try (Session session = driver.session()) {
                session.run("MERGE(p:fbPage { fb_id: 'philamuseum' }) SET p.title = 'muzeu 1'");
                session.run("MATCH(p:fbPage { fb_id: 'philamuseum' }) RETURN p.title");
            }
        }
    }

    @Test
    public void testPerformance() {
        Driver driver = getDriver();
        Session session = driver.session();

        StatementResult r = session.run("create (d:document) set d.title='séparée' ");

        long s2 = System.currentTimeMillis();
        while (r.hasNext()) {
            System.out.println(r.next().asMap());
        }
        long e2 = System.currentTimeMillis();
        System.out.println(e2 - s2);

        session.close();
        driver.close();
    }

    @Test
    public void test2() {
        Driver driver = getDriver();
        Session session = driver.session();

        StatementResult r = session.run("match (d:document) return d.title");

        long s2 = System.currentTimeMillis();
        while (r.hasNext()) {
            System.out.println(r.next().asMap());
        }
        long e2 = System.currentTimeMillis();
        System.out.println(e2 - s2);

        session.close();
        driver.close();
    }

    @Test
    public void testCreatePerformance() throws Exception {
        List<Map<String, Object>> batch = new ArrayList<>();
        for (int i=0; i<25_000; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("title", "Row "+i);
            row.put("source", "Source "+i);
            row.put("desc", "Source "+i);
            row.put("p2", "Source "+i);
            row.put("p3", "Source "+i);
            row.put("p4", "Source "+i);
            row.put("p5", "Source "+i);
            batch.add(row);
        }

        String query = "UNWIND $batch as row CREATE (n:document) SET n += row";
        long s2 = System.currentTimeMillis();
        try (Driver driver = getDriver(); Session session = driver.session()) {
            session.run(query, Values.parameters("batch", batch));
        }
        long e2 = System.currentTimeMillis();
        System.out.println(e2 - s2);
    }

    @Test
    public void testUnwind() throws Exception {
        String query = "UNWIND $work as work_row\n" +
                "MATCH(u:fbUser { fb_id: $fbId })\n" +
                "MERGE(p:fbPage { fb_id: work_row.fb_id })\n" +
                "ON CREATE SET p.title = work_row.name\n" +
                "MERGE (u)-[:work]->(p)" +
                "RETURN p";

        List<Map<String, Object>> work = new ArrayList<>();
        Map<String, Object> w = new HashMap<>();
        w.put("fb_id", "Ljungby");
        w.put("name", "Ljungby kommun");
        work.add(w);

        Driver driver = getDriver();
        Session session = driver.session();
        StatementResult r = session.run(query, Values.parameters(
                "work", work,
                "fbId", "ciugulea"
        ));
        if (r.hasNext()) {
            r.next();
            int i=0;
        }
        driver.close();
    }

    @Test
    public void testMatchMutithreaded() throws Exception {
        int NO_THREADS = 30;

        ExecutorService executorService = Executors.newFixedThreadPool(NO_THREADS);
        Collection<Callable<Boolean>> threads = new ArrayList<>();

        for (int i=0; i<NO_THREADS; i++) {
            String val = String.valueOf(System.nanoTime());
            threads.add(() -> {
                try {
                    Driver driver = getDriver();
                    Session session = driver.session();
                    session.run(
                            "MERGE (p:person { national_identifier: $p }) SET p.source = 'test'",
                            Values.parameters("p", val)
                    );
                    driver.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                return true;
            });
        }

        executorService.invokeAll(threads, 1, TimeUnit.DAYS);
    }

    @Test
    public void testConnection() throws Exception {
        Driver driver = getDriver();
        Session session = driver.session(
                SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.WRITE)
                        .build()
        );

        driver.close();
    }

    public static Driver getDriver() {
        URI uri = URI.create("bolt://core-1384056566956822528.cloud.bigconnect.io:10242");
        Driver driver = BigConnect.driver(uri, basic( "admin", "admin"), secureBuilder().build());
        driver.verifyConnectivity();
        return driver;
    }

    public static Config.ConfigBuilder secureBuilder() {
        return Config.builder()
                .withEncryption()
                .withConnectionTimeout(5, TimeUnit.SECONDS)
                .withTrustStrategy(Config.TrustStrategy.trustAllCertificates())
                .withLogging(slf4j());
    }
}
