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
package com.mware.bigconnect.driver.internal.messaging.v1;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.TransactionConfig;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.internal.BookmarkHolder;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.async.ExplicitTransaction;
import com.mware.bigconnect.driver.internal.cursor.AsyncResultCursorOnlyFactory;
import com.mware.bigconnect.driver.internal.cursor.StatementResultCursorFactory;
import com.mware.bigconnect.driver.internal.handlers.*;
import com.mware.bigconnect.driver.internal.messaging.BoltProtocol;
import com.mware.bigconnect.driver.internal.messaging.Message;
import com.mware.bigconnect.driver.internal.messaging.MessageFormat;
import com.mware.bigconnect.driver.internal.messaging.request.InitMessage;
import com.mware.bigconnect.driver.internal.messaging.request.PullAllMessage;
import com.mware.bigconnect.driver.internal.messaging.request.RunMessage;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;
import com.mware.bigconnect.driver.internal.util.Futures;
import com.mware.bigconnect.driver.internal.util.MetadataExtractor;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.util.Collections.emptyMap;
import static com.mware.bigconnect.driver.Values.ofValue;
import static com.mware.bigconnect.driver.Values.value;
import static com.mware.bigconnect.driver.internal.async.connection.ChannelAttributes.messageDispatcher;
import static com.mware.bigconnect.driver.internal.messaging.request.MultiDatabaseUtil.assertEmptyDatabaseName;
import static com.mware.bigconnect.driver.internal.util.Iterables.newHashMapWithSize;

public class BoltProtocolV1 implements BoltProtocol
{
    public static final int VERSION = 1;

    public static final BoltProtocol INSTANCE = new BoltProtocolV1();

    public static final MetadataExtractor METADATA_EXTRACTOR = new MetadataExtractor( "result_available_after", "result_consumed_after" );

    private static final String BEGIN_QUERY = "BEGIN";
    private static final Message BEGIN_MESSAGE = new RunMessage( BEGIN_QUERY );
    private static final Message COMMIT_MESSAGE = new RunMessage( "COMMIT" );
    private static final Message ROLLBACK_MESSAGE = new RunMessage( "ROLLBACK" );

    @Override
    public MessageFormat createMessageFormat()
    {
        return new MessageFormatV1();
    }

    @Override
    public void initializeChannel(String userAgent, Map<String,Value> authToken, ChannelPromise channelInitializedPromise )
    {
        Channel channel = channelInitializedPromise.channel();

        InitMessage message = new InitMessage( userAgent, authToken );
        InitResponseHandler handler = new InitResponseHandler( channelInitializedPromise );

        messageDispatcher( channel ).enqueue( handler );
        channel.writeAndFlush( message, channel.voidPromise() );
    }

    @Override
    public void prepareToCloseChannel( Channel channel )
    {
        // left empty on purpose.
    }

    @Override
    public CompletionStage<Void> beginTransaction(Connection connection, InternalBookmark bookmark, TransactionConfig config )
    {
        try
        {
            verifyBeforeTransaction( config, connection.databaseName() );
        }
        catch ( Exception error )
        {
            return Futures.failedFuture( error );
        }

        if ( bookmark.isEmpty() )
        {
            connection.write(
                    BEGIN_MESSAGE, NoOpResponseHandler.INSTANCE,
                    PullAllMessage.PULL_ALL, NoOpResponseHandler.INSTANCE );

            return Futures.completedWithNull();
        }
        else
        {
            CompletableFuture<Void> beginTxFuture = new CompletableFuture<>();
            connection.writeAndFlush(
                    new RunMessage( BEGIN_QUERY, SingleBookmarkHelper.asBeginTransactionParameters( bookmark ) ), NoOpResponseHandler.INSTANCE,
                    PullAllMessage.PULL_ALL, new BeginTxResponseHandler( beginTxFuture ) );

            return beginTxFuture;
        }
    }



    @Override
    public CompletionStage<InternalBookmark> commitTransaction(Connection connection )
    {
        CompletableFuture<InternalBookmark> commitFuture = new CompletableFuture<>();

        ResponseHandler pullAllHandler = new CommitTxResponseHandler( commitFuture );
        connection.writeAndFlush(
                COMMIT_MESSAGE, NoOpResponseHandler.INSTANCE,
                PullAllMessage.PULL_ALL, pullAllHandler );

        return commitFuture;
    }

    @Override
    public CompletionStage<Void> rollbackTransaction(Connection connection )
    {
        CompletableFuture<Void> rollbackFuture = new CompletableFuture<>();

        ResponseHandler pullAllHandler = new RollbackTxResponseHandler( rollbackFuture );
        connection.writeAndFlush(
                ROLLBACK_MESSAGE, NoOpResponseHandler.INSTANCE,
                PullAllMessage.PULL_ALL, pullAllHandler );

        return rollbackFuture;
    }

    @Override
    public StatementResultCursorFactory runInAutoCommitTransaction( Connection connection, Statement statement,
            BookmarkHolder bookmarkHolder, TransactionConfig config, boolean waitForRunResponse )
    {
        // bookmarks are ignored for auto-commit transactions in this version of the protocol
        verifyBeforeTransaction( config, connection.databaseName() );
        return buildResultCursorFactory( connection, statement, null, waitForRunResponse );
    }

    @Override
    public StatementResultCursorFactory runInExplicitTransaction( Connection connection, Statement statement, ExplicitTransaction tx,
            boolean waitForRunResponse )
    {
        return buildResultCursorFactory( connection, statement, tx, waitForRunResponse );
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    private static StatementResultCursorFactory buildResultCursorFactory( Connection connection, Statement statement,
            ExplicitTransaction tx, boolean waitForRunResponse )
    {
        String query = statement.text();
        Map<String,Value> params = statement.parameters().asMap( ofValue() );

        RunMessage runMessage = new RunMessage( query, params );
        RunResponseHandler runHandler = new RunResponseHandler( METADATA_EXTRACTOR );
        AbstractPullAllResponseHandler pullAllHandler = PullHandlers.newBoltV1PullAllHandler( statement, runHandler, connection, tx );

        return new AsyncResultCursorOnlyFactory( connection, runMessage, runHandler, pullAllHandler, waitForRunResponse );
    }

    private void verifyBeforeTransaction( TransactionConfig config, String databaseName )
    {
        if ( config != null && !config.isEmpty() )
        {
            throw txConfigNotSupported();
        }
        assertEmptyDatabaseName( databaseName, version() );
    }

    private static ClientException txConfigNotSupported()
    {
        return new ClientException( "Driver is connected to the database that does not support transaction configuration. " );
    }

    static class SingleBookmarkHelper
    {
        private static final String BOOKMARK_PREFIX = "bc:bookmark:v1:tx";
        private static final long UNKNOWN_BOOKMARK_VALUE = -1;

        static Map<String,Value> asBeginTransactionParameters(InternalBookmark bookmark )
        {
            if ( bookmark.isEmpty() )
            {
                return emptyMap();
            }

            // Driver sends {bookmark: "max", bookmarks: ["one", "two", "max"]} instead of simple
            // {bookmarks: ["one", "two", "max"]} for backwards compatibility reasons. Old servers can only accept single
            // bookmark that is why driver has to parse and compare given list of bookmarks. This functionality will
            // eventually be removed.
            Map<String,Value> parameters = newHashMapWithSize( 1 );
            parameters.put( "bookmark", value( maxBookmark( bookmark.values() ) ) );
            parameters.put( "bookmarks", value( bookmark.values() ) );
            return parameters;
        }

        private static String maxBookmark(Iterable<String> bookmarks )
        {
            if ( bookmarks == null )
            {
                return null;
            }

            Iterator<String> iterator = bookmarks.iterator();

            if ( !iterator.hasNext() )
            {
                return null;
            }

            String maxBookmark = iterator.next();
            long maxValue = bookmarkValue( maxBookmark );

            while ( iterator.hasNext() )
            {
                String bookmark = iterator.next();
                long value = bookmarkValue( bookmark );

                if ( value > maxValue )
                {
                    maxBookmark = bookmark;
                    maxValue = value;
                }
            }

            return maxBookmark;
        }

        private static long bookmarkValue( String value )
        {
            if ( value != null && value.startsWith( BOOKMARK_PREFIX ) )
            {
                try
                {
                    return Long.parseLong( value.substring( BOOKMARK_PREFIX.length() ) );
                }
                catch ( NumberFormatException e )
                {
                    return UNKNOWN_BOOKMARK_VALUE;
                }
            }
            return UNKNOWN_BOOKMARK_VALUE;
        }
    }
}
