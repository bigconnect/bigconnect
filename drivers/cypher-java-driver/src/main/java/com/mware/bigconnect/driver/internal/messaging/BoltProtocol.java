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
package com.mware.bigconnect.driver.internal.messaging;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import com.mware.bigconnect.driver.*;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.internal.BookmarkHolder;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.async.ExplicitTransaction;
import com.mware.bigconnect.driver.internal.cursor.StatementResultCursorFactory;
import com.mware.bigconnect.driver.internal.messaging.v1.BoltProtocolV1;
import com.mware.bigconnect.driver.internal.messaging.v2.BoltProtocolV2;
import com.mware.bigconnect.driver.internal.messaging.v3.BoltProtocolV3;
import com.mware.bigconnect.driver.internal.messaging.v4.BoltProtocolV4;
import com.mware.bigconnect.driver.internal.spi.Connection;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.protocolVersion;

public interface BoltProtocol
{
    /**
     * Instantiate {@link MessageFormat} used by this Bolt protocol verison.
     *
     * @return new message format.
     */
    MessageFormat createMessageFormat();

    /**
     * Initialize channel after it is connected and handshake selected this protocol version.
     *
     * @param userAgent the user agent string.
     * @param authToken the authentication token.
     * @param channelInitializedPromise the promise to be notified when initialization is completed.
     */
    void initializeChannel(String userAgent, Map<String, Value> authToken, ChannelPromise channelInitializedPromise);

    /**
     * Prepare to close channel before it is closed.
     * @param channel the channel to close.
     */
    void prepareToCloseChannel(Channel channel);

    /**
     * Begin an explicit transaction.
     *
     * @param connection the connection to use.
     * @param bookmark the bookmarks. Never null, should be {@link InternalBookmark#empty()} when absent.
     * @param config the transaction configuration. Never null, should be {@link TransactionConfig#empty()} when absent.
     * @return a completion stage completed when transaction is started or completed exceptionally when there was a failure.
     */
    CompletionStage<Void> beginTransaction(Connection connection, InternalBookmark bookmark, TransactionConfig config);

    /**
     * Commit the explicit transaction.
     *
     * @param connection the connection to use.
     * @return a completion stage completed with a bookmark when transaction is committed or completed exceptionally when there was a failure.
     */
    CompletionStage<InternalBookmark> commitTransaction(Connection connection);

    /**
     * Rollback the explicit transaction.
     *
     * @param connection the connection to use.
     * @return a completion stage completed when transaction is rolled back or completed exceptionally when there was a failure.
     */
    CompletionStage<Void> rollbackTransaction(Connection connection);

    /**
     * Execute the given statement in an aut-commit transaction, i.e. {@link Session#run(Statement)}.
     *
     * @param connection the network connection to use.
     * @param statement the cypher to execute.
     * @param bookmarkHolder the bookmarksHolder that keeps track of the current bookmark and can be updated with a new bookmark.
     * @param config the transaction config for the implicitly started auto-commit transaction.
     * @param waitForRunResponse {@code true} for async query execution and {@code false} for blocking query
     * execution. Makes returned cursor stage be chained after the RUN response arrives. Needed to have statement
     * keys populated.
     * @return stage with cursor.
     */
    StatementResultCursorFactory runInAutoCommitTransaction(Connection connection, Statement statement,
                                                            BookmarkHolder bookmarkHolder, TransactionConfig config, boolean waitForRunResponse);

    /**
     * Execute the given statement in a running explicit transaction, i.e. {@link Transaction#run(Statement)}.
     *
     * @param connection the network connection to use.
     * @param statement the cypher to execute.
     * @param tx the transaction which executes the query.
     * @param waitForRunResponse {@code true} for async query execution and {@code false} for blocking query
     * execution. Makes returned cursor stage be chained after the RUN response arrives. Needed to have statement
     * keys populated.
     * @return stage with cursor.
     */
    StatementResultCursorFactory runInExplicitTransaction(Connection connection, Statement statement, ExplicitTransaction tx,
                                                          boolean waitForRunResponse);

    /**
     * Returns the protocol version. It can be used for version specific error messages.
     * @return the protocol version.
     */
    int version();

    /**
     * Obtain an instance of the protocol for the given channel.
     *
     * @param channel the channel to get protocol for.
     * @return the protocol.
     * @throws ClientException when unable to find protocol version for the given channel.
     */
    static BoltProtocol forChannel(Channel channel)
    {
        return forVersion( protocolVersion( channel ) );
    }

    /**
     * Obtain an instance of the protocol for the given channel.
     *
     * @param version the version of the protocol.
     * @return the protocol.
     * @throws ClientException when unable to find protocol with the given version.
     */
    static BoltProtocol forVersion(int version)
    {
        switch ( version )
        {
        case BoltProtocolV1.VERSION:
            return BoltProtocolV1.INSTANCE;
        case BoltProtocolV2.VERSION:
            return BoltProtocolV2.INSTANCE;
        case BoltProtocolV3.VERSION:
            return BoltProtocolV3.INSTANCE;
        case BoltProtocolV4.VERSION:
            return BoltProtocolV4.INSTANCE;
        default:
            throw new ClientException( "Unknown protocol version: " + version );
        }
    }
}
