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
package com.mware.bigconnect.driver.internal.messaging.v3;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.TransactionConfig;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.BookmarkHolder;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.async.ExplicitTransaction;
import com.mware.bigconnect.driver.internal.cursor.AsyncResultCursorOnlyFactory;
import com.mware.bigconnect.driver.internal.cursor.StatementResultCursorFactory;
import com.mware.bigconnect.driver.internal.handlers.*;
import com.mware.bigconnect.driver.internal.messaging.BoltProtocol;
import com.mware.bigconnect.driver.internal.messaging.MessageFormat;
import com.mware.bigconnect.driver.internal.messaging.request.BeginMessage;
import com.mware.bigconnect.driver.internal.messaging.request.GoodbyeMessage;
import com.mware.bigconnect.driver.internal.messaging.request.HelloMessage;
import com.mware.bigconnect.driver.internal.messaging.request.RunWithMetadataMessage;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.internal.util.MetadataExtractor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static com.mware.bigconnect.driver.internal.handlers.PullHandlers.newBoltV3PullAllHandler;
import static com.mware.bigconnect.driver.internal.messaging.request.CommitMessage.COMMIT;
import static com.mware.bigconnect.driver.internal.messaging.request.MultiDatabaseUtil.assertEmptyDatabaseName;
import static com.mware.bigconnect.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static com.mware.bigconnect.driver.internal.messaging.request.RunWithMetadataMessage.autoCommitTxRunMessage;
import static com.mware.bigconnect.driver.internal.messaging.request.RunWithMetadataMessage.explicitTxRunMessage;

public class BoltProtocolV3 implements BoltProtocol
{
    public static final int VERSION = 3;

    public static final BoltProtocol INSTANCE = new BoltProtocolV3();

    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor( "t_first", "t_last" );

    @Override
    public MessageFormat createMessageFormat()
    {
        return new MessageFormatV3();
    }

    @Override
    public void initializeChannel(String userAgent, Map<String,Value> authToken, ChannelPromise channelInitializedPromise )
    {
        Channel channel = channelInitializedPromise.channel();

        HelloMessage message = new HelloMessage( userAgent, authToken );
        HelloResponseHandler handler = new HelloResponseHandler( channelInitializedPromise );

        messageDispatcher( channel ).enqueue( handler );
        channel.writeAndFlush( message, channel.voidPromise() );
    }

    @Override
    public void prepareToCloseChannel( Channel channel )
    {
        GoodbyeMessage message = GoodbyeMessage.GOODBYE;
        messageDispatcher( channel ).enqueue( NoOpResponseHandler.INSTANCE );
        channel.writeAndFlush( message, channel.voidPromise() );
    }

    @Override
    public CompletionStage<Void> beginTransaction(Connection connection, InternalBookmark bookmark, TransactionConfig config )
    {
        try
        {
            verifyDatabaseNameBeforeTransaction( connection.databaseName() );
        }
        catch ( Exception error )
        {
            return Futures.failedFuture( error );
        }

        BeginMessage beginMessage = new BeginMessage( bookmark, config, connection.databaseName(), connection.mode() );

        if ( bookmark.isEmpty() )
        {
            connection.write( beginMessage, NoOpResponseHandler.INSTANCE );
            return Futures.completedWithNull();
        }
        else
        {
            CompletableFuture<Void> beginTxFuture = new CompletableFuture<>();
            connection.writeAndFlush( beginMessage, new BeginTxResponseHandler( beginTxFuture ) );
            return beginTxFuture;
        }
    }

    @Override
    public CompletionStage<InternalBookmark> commitTransaction(Connection connection )
    {
        CompletableFuture<InternalBookmark> commitFuture = new CompletableFuture<>();
        connection.writeAndFlush( COMMIT, new CommitTxResponseHandler( commitFuture ) );
        return commitFuture;
    }

    @Override
    public CompletionStage<Void> rollbackTransaction(Connection connection )
    {
        CompletableFuture<Void> rollbackFuture = new CompletableFuture<>();
        connection.writeAndFlush( ROLLBACK, new RollbackTxResponseHandler( rollbackFuture ) );
        return rollbackFuture;
    }

    @Override
    public StatementResultCursorFactory runInAutoCommitTransaction( Connection connection, Statement statement,
            BookmarkHolder bookmarkHolder, TransactionConfig config, boolean waitForRunResponse )
    {
        verifyDatabaseNameBeforeTransaction( connection.databaseName() );
        RunWithMetadataMessage runMessage =
                autoCommitTxRunMessage( statement, config, connection.databaseName(), connection.mode(), bookmarkHolder.getBookmark() );
        return buildResultCursorFactory( connection, statement, bookmarkHolder, null, runMessage, waitForRunResponse );
    }

    @Override
    public StatementResultCursorFactory runInExplicitTransaction( Connection connection, Statement statement, ExplicitTransaction tx,
            boolean waitForRunResponse )
    {
        RunWithMetadataMessage runMessage = explicitTxRunMessage( statement );
        return buildResultCursorFactory( connection, statement, BookmarkHolder.NO_OP, tx, runMessage, waitForRunResponse );
    }

    protected StatementResultCursorFactory buildResultCursorFactory( Connection connection, Statement statement, BookmarkHolder bookmarkHolder,
            ExplicitTransaction tx, RunWithMetadataMessage runMessage, boolean waitForRunResponse )
    {
        RunResponseHandler runHandler = new RunResponseHandler( METADATA_EXTRACTOR );
        AbstractPullAllResponseHandler pullHandler = newBoltV3PullAllHandler( statement, runHandler, connection, bookmarkHolder, tx );

        return new AsyncResultCursorOnlyFactory( connection, runMessage, runHandler, pullHandler, waitForRunResponse );
    }

    protected void verifyDatabaseNameBeforeTransaction( String databaseName )
    {
        assertEmptyDatabaseName( databaseName, version() );
    }

    @Override
    public int version()
    {
        return VERSION;
    }
}
