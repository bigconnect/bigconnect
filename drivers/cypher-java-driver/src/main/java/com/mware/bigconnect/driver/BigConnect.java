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

import com.mware.bigconnect.driver.exceptions.ServiceUnavailableException;
import com.mware.bigconnect.driver.internal.DriverFactory;
import com.mware.bigconnect.driver.internal.cluster.RoutingSettings;
import com.mware.bigconnect.driver.internal.retry.RetrySettings;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static com.mware.bigconnect.driver.internal.DriverFactory.BC_URI_SCHEME;
import static com.mware.bigconnect.driver.internal.DriverFactory.BOLT_ROUTING_URI_SCHEME;

/**
 * Creates {@link Driver drivers}, optionally letting you {@link #driver(URI, Config)} to configure them.
 *
 * @see Driver
 * @since 1.0
 */
public class BigConnect {
    private static final String LOGGER_NAME = BigConnect.class.getSimpleName();
    private static final List<String> VALID_ROUTING_URIS = Arrays.asList(BOLT_ROUTING_URI_SCHEME, BC_URI_SCHEME);

    /**
     * Return a driver for a BigConnect instance with the default configuration settings
     *
     * @param uri the URL to a BigConnect instance
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri) {
        return driver(uri, Config.defaultConfig());
    }

    /**
     * Return a driver for a BigConnect instance with the default configuration settings
     *
     * @param uri the URL to a BigConnect instance
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri) {
        return driver(uri, Config.defaultConfig());
    }

    /**
     * Return a driver for a BigConnect instance with custom configuration.
     *
     * @param uri    the URL to a BigConnect instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri, Config config) {
        return driver(uri, AuthTokens.none(), config);
    }

    /**
     * Return a driver for a BigConnect instance with custom configuration.
     *
     * @param uri    the URL to a BigConnect instance
     * @param config user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri, Config config) {
        return driver(URI.create(uri), config);
    }

    /**
     * Return a driver for a BigConnect instance with the default configuration settings
     *
     * @param uri       the URL to a BigConnect instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri, AuthToken authToken) {
        return driver(uri, authToken, Config.defaultConfig());
    }

    /**
     * Return a driver for a BigConnect instance with the default configuration settings
     *
     * @param uri       the URL to a BigConnect instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri, AuthToken authToken) {
        return driver(uri, authToken, Config.defaultConfig());
    }

    /**
     * Return a driver for a BigConnect instance with custom configuration.
     *
     * @param uri       the URL to a BigConnect instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config    user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(String uri, AuthToken authToken, Config config) {
        return driver(URI.create(uri), authToken, config);
    }

    /**
     * Return a driver for a BigConnect instance with custom configuration.
     *
     * @param uri       the URL to a BigConnect instance
     * @param authToken authentication to use, see {@link AuthTokens}
     * @param config    user defined configuration
     * @return a new driver to the database instance specified by the URL
     */
    public static Driver driver(URI uri, AuthToken authToken, Config config) {
        config = getOrDefault(config);
        RoutingSettings routingSettings = config.routingSettings();
        RetrySettings retrySettings = config.retrySettings();

        return new DriverFactory().newInstance(uri, authToken, routingSettings, retrySettings, config);
    }

    /**
     * Try to create a routing driver from the <b>first</b> available address.
     * This is wrapper for the {@link #driver} method that finds the <b>first</b>
     * server to respond positively.
     *
     * @param routingUris an {@link Iterable} of server {@link URI}s for BigConnect instances. All given URIs should
     *                    have 'bolt+routing' or 'bc' scheme.
     * @param authToken   authentication to use, see {@link AuthTokens}
     * @param config      user defined configuration
     * @return a new driver instance
     */
    public static Driver routingDriver(Iterable<URI> routingUris, AuthToken authToken, Config config) {
        assertRoutingUris(routingUris);
        Logger log = createLogger(config);

        for (URI uri : routingUris) {
            final Driver driver = driver(uri, authToken, config);
            try {
                driver.verifyConnectivity();
                return driver;
            } catch (ServiceUnavailableException e) {
                log.warn("Unable to create routing driver for URI: " + uri, e);
                closeDriver(driver, uri, log);
            } catch (Throwable e) {
                // for any other errors, we first close the driver and then rethrow the original error out.
                closeDriver(driver, uri, log);
                throw e;
            }
        }

        throw new ServiceUnavailableException("Failed to discover an available server");
    }

    private static void closeDriver(Driver driver, URI uri, Logger log) {
        try {
            driver.close();
        } catch (Throwable closeError) {
            log.warn("Unable to close driver towards URI: " + uri, closeError);
        }
    }

    private static void assertRoutingUris(Iterable<URI> uris) {
        for (URI uri : uris) {
            if (!VALID_ROUTING_URIS.contains(uri.getScheme())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Illegal URI scheme, expected URI scheme '%s' to be among '%s'", uri.getScheme(), VALID_ROUTING_URIS.toString()));
            }
        }
    }

    private static Logger createLogger(Config config) {
        Logging logging = getOrDefault(config).logging();
        return logging.getLog(LOGGER_NAME);
    }

    private static Config getOrDefault(Config config) {
        return config != null ? config : Config.defaultConfig();
    }
}
