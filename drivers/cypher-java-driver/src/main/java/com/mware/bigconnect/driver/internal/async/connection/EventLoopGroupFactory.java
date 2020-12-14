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
package com.mware.bigconnect.driver.internal.async.connection;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocalThread;
import com.mware.bigconnect.driver.Session;
import com.mware.bigconnect.driver.async.AsyncSession;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * Manages creation of Netty {@link EventLoopGroup}s, which are basically {@link Executor}s that perform IO operations.
 */
public final class EventLoopGroupFactory
{
    private static final String THREAD_NAME_PREFIX = "BigConnectDriverIO";
    private static final int THREAD_PRIORITY = Thread.MAX_PRIORITY;

    private EventLoopGroupFactory()
    {
    }

    /**
     * Get class of {@link Channel} for {@link Bootstrap#channel(Class)} method.
     *
     * @return class of the channel, which should be consistent with {@link EventLoopGroup}s returned by
     * {@link #newEventLoopGroup()} and {@link #newEventLoopGroup(int)}.
     */
    public static Class<? extends Channel> channelClass()
    {
        return NioSocketChannel.class;
    }

    /**
     * Create new {@link EventLoopGroup} with specified thread count. Returned group should by given to
     * {@link Bootstrap#group(EventLoopGroup)}.
     *
     * @param threadCount amount of IO threads for the new group.
     * @return new group consistent with channel class returned by {@link #channelClass()}.
     */
    public static EventLoopGroup newEventLoopGroup( int threadCount )
    {
        return new DriverEventLoopGroup( threadCount );
    }

    /**
     * Create new {@link EventLoopGroup} with default thread count. Returned group should by given to
     * {@link Bootstrap#group(EventLoopGroup)}.
     *
     * @return new group consistent with channel class returned by {@link #channelClass()}.
     */
    public static EventLoopGroup newEventLoopGroup()
    {
        return new DriverEventLoopGroup();
    }

    /**
     * Assert that current thread is not an event loop used for async IO operations. This check is needed because
     * blocking API methods like {@link Session#run(String)} are implemented on top of corresponding async API methods
     * like {@link AsyncSession#runAsync(String)} using basically {@link Future#get()} calls. Deadlocks might happen when IO
     * thread executes blocking API call and has to wait for itself to read from the network.
     *
     * @throws IllegalStateException when current thread is an event loop IO thread.
     */
    public static void assertNotInEventLoopThread() throws IllegalStateException
    {
        if ( isEventLoopThread( Thread.currentThread() ) )
        {
            throw new IllegalStateException(
                    "Blocking operation can't be executed in IO thread because it might result in a deadlock. " +
                    "Please do not use blocking API when chaining futures returned by async API methods." );
        }
    }

    /**
     * Check if given thread is an event loop IO thread.
     *
     * @param thread the thread to check.
     * @return {@code true} when given thread belongs to the event loop, {@code false} otherwise.
     */
    public static boolean isEventLoopThread( Thread thread )
    {
        return thread instanceof DriverThread;
    }

    /**
     * Same as {@link NioEventLoopGroup} but uses a different {@link ThreadFactory} that produces threads of
     * {@link DriverThread} class. Such threads can be recognized by {@link #assertNotInEventLoopThread()}.
     */
    private static class DriverEventLoopGroup extends NioEventLoopGroup
    {
        DriverEventLoopGroup()
        {
        }

        DriverEventLoopGroup( int nThreads )
        {
            super( nThreads );
        }

        @Override
        protected ThreadFactory newDefaultThreadFactory()
        {
            return new DriverThreadFactory();
        }
    }

    /**
     * Same as {@link DefaultThreadFactory} created by {@link NioEventLoopGroup} by default, except produces threads of
     * {@link DriverThread} class. Such threads can be recognized by {@link #assertNotInEventLoopThread()}.
     */
    private static class DriverThreadFactory extends DefaultThreadFactory
    {
        DriverThreadFactory()
        {
            super( THREAD_NAME_PREFIX, THREAD_PRIORITY );
        }

        @Override
        protected Thread newThread(Runnable r, String name )
        {
            return new DriverThread( threadGroup, r, name );
        }
    }

    /**
     * Same as default thread created by {@link DefaultThreadFactory} except this dedicated class can be easily
     * recognized by {@link #assertNotInEventLoopThread()}.
     */
    private static class DriverThread extends FastThreadLocalThread
    {
        DriverThread(ThreadGroup group, Runnable target, String name )
        {
            super( group, target, name );
        }
    }
}
