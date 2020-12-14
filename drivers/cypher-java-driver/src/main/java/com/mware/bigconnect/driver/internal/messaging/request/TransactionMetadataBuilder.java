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
package com.mware.bigconnect.driver.internal.messaging.request;

import com.mware.bigconnect.driver.AccessMode;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.util.Iterables;

import java.time.Duration;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static com.mware.bigconnect.driver.Values.value;
import static com.mware.bigconnect.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;

public class TransactionMetadataBuilder
{
    private static final String BOOKMARKS_METADATA_KEY = "bookmarks";
    private static final String DATABASE_NAME_KEY = "db";
    private static final String TX_TIMEOUT_METADATA_KEY = "tx_timeout";
    private static final String TX_METADATA_METADATA_KEY = "tx_metadata";
    private static final String MODE_KEY = "mode";
    private static final String MODE_READ_VALUE = "r";

    public static Map<String,Value> buildMetadata(Duration txTimeout, Map<String,Value> txMetadata, AccessMode mode, InternalBookmark bookmark )
    {
        return buildMetadata( txTimeout, txMetadata, ABSENT_DB_NAME, mode, bookmark );
    }

    public static Map<String,Value> buildMetadata(Duration txTimeout, Map<String,Value> txMetadata, String databaseName, AccessMode mode, InternalBookmark bookmark )
    {
        boolean bookmarksPresent = bookmark != null && !bookmark.isEmpty();
        boolean txTimeoutPresent = txTimeout != null;
        boolean txMetadataPresent = txMetadata != null && !txMetadata.isEmpty();
        boolean accessModePresent = mode == AccessMode.READ;
        boolean databaseNamePresent = databaseName != null && !databaseName.equals( ABSENT_DB_NAME );

        if ( !bookmarksPresent && !txTimeoutPresent && !txMetadataPresent && !accessModePresent && !databaseNamePresent )
        {
            return emptyMap();
        }

        Map<String,Value> result = Iterables.newHashMapWithSize( 5 );

        if ( bookmarksPresent )
        {
            result.put( BOOKMARKS_METADATA_KEY, value( bookmark.values() ) );
        }
        if ( txTimeoutPresent )
        {
            result.put( TX_TIMEOUT_METADATA_KEY, value( txTimeout.toMillis() ) );
        }
        if ( txMetadataPresent )
        {
            result.put( TX_METADATA_METADATA_KEY, value( txMetadata ) );
        }
        if( accessModePresent )
        {
            result.put( MODE_KEY, value( MODE_READ_VALUE ) );
        }
        if ( databaseNamePresent ) // only sent if the database name is different from absent
        {
            result.put( DATABASE_NAME_KEY, value( databaseName ) );
        }

        return result;
    }
}
