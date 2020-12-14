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

import com.mware.bigconnect.driver.async.AsyncSession;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.reactive.RxSession;
import com.mware.bigconnect.driver.types.TypeSystem;
import com.mware.bigconnect.driver.util.Experimental;

import java.util.concurrent.CompletionStage;

/**
 * Accessor for a specific BigConnect graph database.
 * <p>
 * Driver implementations are typically thread-safe, act as a template
 * for session creation and host a connection pool. All configuration
 * and authentication settings are held immutably by the Driver. Should
 * different settings be required, a new Driver instance should be created.
 * <p>
 * A driver maintains a connection pool for each remote BigConnect server. Therefore
 * the most efficient way to make use of a Driver is to use the same instance
 * across the application.
 * <p>
 * To construct a new Driver, use one of the
 * {@link BigConnect#driver(String, AuthToken) GraphDatabase.driver} methods.
 * The <a href="https://tools.ietf.org/html/rfc3986">URI</a> passed to
 * this method determines the type of Driver created.
 * <br>
 * <table border="1" style="border-collapse: collapse">
 *     <caption>Available schemes and drivers</caption>
 *     <thead>
 *         <tr><th>URI Scheme</th><th>Driver</th></tr>
 *     </thead>
 *     <tbody>
 *         <tr>
 *             <td><code>bolt</code></td>
 *             <td>Direct driver: connects directly to the host and port specified in the URI.</td>
 *         </tr>
 *         <tr>
 *              <td><code>bolt+routing</code> or <code>bc</code></td>
 *             <td>Routing driver: can automatically discover members of a Causal Cluster and route {@link Session sessions} based on {@link AccessMode}.</td>
 *         </tr>
 *     </tbody>
 * </table>
 *
 * @since 1.0 (Modified and Added {@link AsyncSession} and {@link RxSession} since 2.0)
 */
public interface Driver extends AutoCloseable
{
    /**
     * Return a flag to indicate whether or not encryption is used for this driver.
     *
     * @return true if the driver requires encryption, false otherwise
     */
    boolean isEncrypted();

    /**
     * Create a new general purpose {@link Session} with default {@link SessionConfig session configuration}.
     * <p>
     * Alias to {@link #session(SessionConfig)}}.
     *
     * @return a new {@link Session} object.
     */
    Session session();

    /**
     * Create a new {@link Session} with a specified {@link SessionConfig session configuration}.
     * Use {@link SessionConfig#forDatabase(String)} to obtain a general purpose session configuration for the specified database.
     * @param sessionConfig specifies session configurations for this session.
     * @return a new {@link Session} object.
     * @see SessionConfig
     */
    Session session(SessionConfig sessionConfig);

    /**
     * Create a new general purpose {@link RxSession} with default {@link SessionConfig session configuration}.
     * The {@link RxSession} provides a reactive way to run queries and process results.
     * <p>
     * Alias to {@link #rxSession(SessionConfig)}}.
     *
     * @return @return a new {@link RxSession} object.
     */
    RxSession rxSession();

    /**
     * Create a new {@link RxSession} with a specified {@link SessionConfig session configuration}.
     * Use {@link SessionConfig#forDatabase(String)} to obtain a general purpose session configuration for the specified database.
     * The {@link RxSession} provides a reactive way to run queries and process results.
     * @param sessionConfig used to customize the session.
     * @return @return a new {@link RxSession} object.
     */
    RxSession rxSession(SessionConfig sessionConfig);

    /**
     * Create a new general purpose {@link AsyncSession} with default {@link SessionConfig session configuration}.
     * The {@link AsyncSession} provides an asynchronous way to run queries and process results.
     * <p>
     * Alias to {@link #asyncSession(SessionConfig)}}.
     *
     * @return @return a new {@link AsyncSession} object.
     */
    AsyncSession asyncSession();

    /**
     * Create a new {@link AsyncSession} with a specified {@link SessionConfig session configuration}.
     * Use {@link SessionConfig#forDatabase(String)} to obtain a general purpose session configuration for the specified database.
     * The {@link AsyncSession} provides an asynchronous way to run queries and process results.
     *
     * @param sessionConfig used to customize the session.
     * @return a new {@link AsyncSession} object.
     */
    AsyncSession asyncSession(SessionConfig sessionConfig);

    /**
     * Close all the resources assigned to this driver, including open connections and IO threads.
     * <p>
     * This operation works the same way as {@link #closeAsync()} but blocks until all resources are closed.
     */
    @Override
    void close();

    /**
     * Close all the resources assigned to this driver, including open connections and IO threads.
     * <p>
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with
     * {@code null} when all resources are closed. It is completed exceptionally if termination fails.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous close.
     */
    CompletionStage<Void> closeAsync();

    /**
     * Returns the driver metrics if metrics reporting is enabled via {@link Config.ConfigBuilder#withDriverMetrics()}.
     * Otherwise a {@link ClientException} will be thrown.
     * @return the driver metrics if enabled.
     * @throws ClientException if the driver metrics reporting is not enabled.
     */
    @Experimental
    Metrics metrics();

    /**
     * Returns true if the driver metrics reporting is enabled via {@link Config.ConfigBuilder#withDriverMetrics()}, otherwise false.
     *
     * @return true if the metrics reporting is enabled.
     */
    @Experimental
    boolean isMetricsEnabled();

    /**
     * This will return the type system supported by the driver.
     * The types supported on a particular server a session is connected against might not contain all of the types defined here.
     *
     * @return type system used by this statement runner for classifying values
     */
    @Experimental
    TypeSystem defaultTypeSystem();

    /**
     * This verifies if the driver can connect to a remote server or a cluster
     * by establishing a network connection with the remote and possibly exchanging a few data before closing the connection.
     *
     * It throws exception if fails to connect. Use the exception to further understand the cause of the connectivity problem.
     * Note: Even if this method throws an exception, the driver still need to be closed via {@link #close()} to free up all resources.
     */
    void verifyConnectivity();

    /**
     * This verifies if the driver can connect to a remote server or cluster
     * by establishing a network connection with the remote and possibly exchanging a few data before closing the connection.
     *
     * This operation is asynchronous and returns a {@link CompletionStage}. This stage is completed with
     * {@code null} when the driver connects to the remote server or cluster successfully.
     * It is completed exceptionally if the driver failed to connect the remote server or cluster.
     * This exception can be used to further understand the cause of the connectivity problem.
     * Note: Even if this method complete exceptionally, the driver still need to be closed via {@link #closeAsync()} to free up all resources.
     *
     * @return a {@link CompletionStage completion stage} that represents the asynchronous verification.
     */
    CompletionStage<Void> verifyConnectivityAsync();
}
