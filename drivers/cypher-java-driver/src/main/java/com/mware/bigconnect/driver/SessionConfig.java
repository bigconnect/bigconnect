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
import com.mware.bigconnect.driver.internal.Bookmark;
import com.mware.bigconnect.driver.reactive.RxSession;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static com.mware.bigconnect.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

/**
 * The session configurations used to configure a session.
 */
public class SessionConfig
{
    private static final SessionConfig EMPTY = builder().build();

    private final Iterable<Bookmark> bookmarks;
    private final AccessMode defaultAccessMode;
    private final String database;

    private SessionConfig( Builder builder )
    {
        this.bookmarks = builder.bookmarks;
        this.defaultAccessMode = builder.defaultAccessMode;
        this.database = builder.database;
    }

    /**
     * Creates a new {@link Builder} used to construct a configuration object.
     *
     * @return a session configuration builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Returns a static {@link SessionConfig} with default values for a general purpose session.
     *
     * @return a session config for a general purpose session.
     */
    public static SessionConfig defaultConfig()
    {
        return EMPTY;
    }

    /**
     * Returns a {@link SessionConfig} for the specified database
     * @param database the database the session binds to.
     * @return a session config for a session for the specified database.
     */
    public static SessionConfig forDatabase( String database )
    {
        return new Builder().withDatabase( database ).build();
    }

    /**
     * Returns the initial bookmarks.
     * First transaction in the session created with this {@link SessionConfig}
     * will ensure that server hosting is at least as up-to-date as the
     * latest transaction referenced by the supplied initial bookmarks.
     *
     * @return the initial bookmarks.
     */
    public Iterable<Bookmark> bookmarks()
    {
        return bookmarks;
    }

    /**
     * The type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     *
     * @return the access mode.
     */
    public AccessMode defaultAccessMode()
    {
        return defaultAccessMode;
    }

    /**
     * The database where the session is going to connect to.
     *
     * @return the nullable database name where the session is going to connect to.
     */
    public Optional<String> database()
    {
        return Optional.ofNullable( database );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        SessionConfig that = (SessionConfig) o;
        return Objects.equals( bookmarks, that.bookmarks ) && defaultAccessMode == that.defaultAccessMode && Objects.equals( database, that.database );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( bookmarks, defaultAccessMode, database );
    }

    @Override
    public String toString()
    {
        return "SessionParameters{" + "bookmarks=" + bookmarks + ", defaultAccessMode=" + defaultAccessMode + ", database='" + database + '\'' + '}';
    }

    /**
     * Builder used to configure {@link SessionConfig} which will be used to create a session.
     */
    public static class Builder
    {
        private Iterable<Bookmark> bookmarks = null;
        private AccessMode defaultAccessMode = AccessMode.WRITE;
        private String database = null;

        private Builder()
        {
        }

        /**
         * Set the initial bookmarks to be used in a session.
         * <p>
         * First transaction in a session will ensure that server hosting is at least as up-to-date as the
         * latest transaction referenced by the supplied bookmarks.
         * The bookmarks can be obtained via {@link Session#lastBookmark()}, {@link AsyncSession#lastBookmark()},
         * and/or {@link RxSession#lastBookmark()}.
         *
         * @param bookmarks a series of initial bookmarks.
         * Both {@code null} value and empty array
         * are permitted, and indicate that the bookmarks do not exist or are unknown.
         * @return this builder.
         */
        public Builder withBookmarks( Bookmark... bookmarks )
        {
            if ( bookmarks == null )
            {
                this.bookmarks = null;
            }
            else
            {
                this.bookmarks = Arrays.asList( bookmarks );
            }
            return this;
        }

        /**
         * Set the initial bookmarks to be used in a session.
         * First transaction in a session will ensure that server hosting is at least as up-to-date as the
         * latest transaction referenced by the supplied bookmarks.
         * The bookmarks can be obtained via {@link Session#lastBookmark()}, {@link AsyncSession#lastBookmark()},
         * and/or {@link RxSession#lastBookmark()}.
         *
         * @param bookmarks initial references to some previous transactions. Both {@code null} value and empty iterable
         * are permitted, and indicate that the bookmarks do not exist or are unknown.
         * @return this builder
         */
        public Builder withBookmarks( Iterable<Bookmark> bookmarks )
        {
            this.bookmarks = bookmarks;
            return this;
        }

        /**
         * Set the type of access required by units of work in this session,
         * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
         * This access mode is used to route transactions in the session to the server who has the right to carry out the specified operations.
         *
         * @param mode access mode.
         * @return this builder.
         */
        public Builder withDefaultAccessMode( AccessMode mode )
        {
            this.defaultAccessMode = mode;
            return this;
        }

        /**
         * Set the database that the newly created session is going to connect to.
         * <p>
         * For connecting to servers that support multi-databases,
         * it is highly recommended to always set the database explicitly in the {@link SessionConfig} for each session.
         * If the database name is not set, then session defaults to connecting to the default database configured in server configuration.
         * <p>
         * For servers that do not support multi-databases, leave this database value unset. The only database will be used instead.
         *
         * @param database the database the session going to connect to. Provided value should not be {@code null}.
         * @return this builder.
         */
        public Builder withDatabase( String database )
        {
            requireNonNull( database, "Database name should not be null." );
            if ( ABSENT_DB_NAME.equals( database ) )
            {
                // Disallow users to use bolt internal value directly. To users, this is totally an illegal database name.
                throw new IllegalArgumentException( String.format( "Illegal database name '%s'.", database ) );
            }
            this.database = database;
            return this;
        }

        public SessionConfig build()
        {
            return new SessionConfig( this );
        }
    }
}
