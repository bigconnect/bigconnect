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
package com.mware.bigconnect.driver.internal.util;

import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.exceptions.UntrustedServerException;
import com.mware.bigconnect.driver.internal.InternalBookmark;
import com.mware.bigconnect.driver.internal.spi.Connection;
import com.mware.bigconnect.driver.internal.summary.*;
import com.mware.bigconnect.driver.summary.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static com.mware.bigconnect.driver.internal.summary.InternalDatabaseInfo.DEFAULT_DATABASE_INFO;
import static com.mware.bigconnect.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

public class MetadataExtractor
{
    public static final int ABSENT_QUERY_ID = -1;
    private final String resultAvailableAfterMetadataKey;
    private final String resultConsumedAfterMetadataKey;

    public MetadataExtractor(String resultAvailableAfterMetadataKey, String resultConsumedAfterMetadataKey )
    {
        this.resultAvailableAfterMetadataKey = resultAvailableAfterMetadataKey;
        this.resultConsumedAfterMetadataKey = resultConsumedAfterMetadataKey;
    }

    public List<String> extractStatementKeys(Map<String,Value> metadata )
    {
        Value keysValue = metadata.get( "fields" );
        if ( keysValue != null )
        {
            if ( !keysValue.isEmpty() )
            {
                List<String> keys = new ArrayList<>( keysValue.size() );
                for ( Value value : keysValue.values() )
                {
                    keys.add( value.asString() );
                }

                return keys;
            }
        }
        return emptyList();
    }

    public long extractQueryId( Map<String,Value> metadata )
    {
        Value statementId = metadata.get( "qid" );
        if ( statementId != null )
        {
            return statementId.asLong();
        }
        return ABSENT_QUERY_ID;
    }


    public long extractResultAvailableAfter( Map<String,Value> metadata )
    {
        Value resultAvailableAfterValue = metadata.get( resultAvailableAfterMetadataKey );
        if ( resultAvailableAfterValue != null )
        {
            return resultAvailableAfterValue.asLong();
        }
        return -1;
    }

    public ResultSummary extractSummary( Statement statement, Connection connection, long resultAvailableAfter, Map<String,Value> metadata )
    {
        ServerInfo serverInfo = new InternalServerInfo( connection.serverAddress(), connection.serverVersion() );
        DatabaseInfo dbInfo = extractDatabaseInfo( metadata );
        return new InternalResultSummary( statement, serverInfo, dbInfo, extractStatementType( metadata ), extractCounters( metadata ), extractPlan( metadata ),
                extractProfiledPlan( metadata ), extractNotifications( metadata ), resultAvailableAfter,
                extractResultConsumedAfter( metadata, resultConsumedAfterMetadataKey ) );
    }

    public static DatabaseInfo extractDatabaseInfo( Map<String,Value> metadata )
    {
        Value dbValue = metadata.get( "db" );
        if ( dbValue == null || dbValue.isNull() )
        {
            return DEFAULT_DATABASE_INFO;
        }
        else
        {
            return new InternalDatabaseInfo( dbValue.asString() );
        }
    }

    public static InternalBookmark extractBookmarks( Map<String,Value> metadata )
    {
        Value bookmarkValue = metadata.get( "bookmark" );
        if ( bookmarkValue != null && !bookmarkValue.isNull() && bookmarkValue.hasType( TYPE_SYSTEM.STRING() ) )
        {
            return InternalBookmark.parse( bookmarkValue.asString() );
        }
        return InternalBookmark.empty();
    }

    public static ServerVersion extractBigConnectServerVersion( Map<String,Value> metadata )
    {
        Value versionValue = metadata.get( "server" );
        if ( versionValue == null || versionValue.isNull() )
        {
            throw new UntrustedServerException( "Server provides no product identifier" );
        }
        else
        {
            ServerVersion server = ServerVersion.version( versionValue.asString() );
            if ( ServerVersion.BIGCONNECT_PRODUCT.equalsIgnoreCase( server.product() ) )
            {
                return server;
            }
            else
            {
                throw new UntrustedServerException( "Server does not identify as a genuine BigConnect instance: '" + server.product() + "'" );
            }
        }
    }

    private static StatementType extractStatementType( Map<String,Value> metadata )
    {
        Value typeValue = metadata.get( "type" );
        if ( typeValue != null )
        {
            return StatementType.fromCode( typeValue.asString() );
        }
        return null;
    }

    private static InternalSummaryCounters extractCounters( Map<String,Value> metadata )
    {
        Value countersValue = metadata.get( "stats" );
        if ( countersValue != null )
        {
            return new InternalSummaryCounters(
                    counterValue( countersValue, "nodes-created" ),
                    counterValue( countersValue, "nodes-deleted" ),
                    counterValue( countersValue, "relationships-created" ),
                    counterValue( countersValue, "relationships-deleted" ),
                    counterValue( countersValue, "properties-set" ),
                    counterValue( countersValue, "labels-added" ),
                    counterValue( countersValue, "labels-removed" ),
                    counterValue( countersValue, "indexes-added" ),
                    counterValue( countersValue, "indexes-removed" ),
                    counterValue( countersValue, "constraints-added" ),
                    counterValue( countersValue, "constraints-removed" )
            );
        }
        return null;
    }

    private static int counterValue( Value countersValue, String name )
    {
        Value value = countersValue.get( name );
        return value.isNull() ? 0 : value.asInt();
    }

    private static Plan extractPlan( Map<String,Value> metadata )
    {
        Value planValue = metadata.get( "plan" );
        if ( planValue != null )
        {
            return InternalPlan.EXPLAIN_PLAN_FROM_VALUE.apply( planValue );
        }
        return null;
    }

    private static ProfiledPlan extractProfiledPlan( Map<String,Value> metadata )
    {
        Value profiledPlanValue = metadata.get( "profile" );
        if ( profiledPlanValue != null )
        {
            return InternalProfiledPlan.PROFILED_PLAN_FROM_VALUE.apply( profiledPlanValue );
        }
        return null;
    }

    private static List<Notification> extractNotifications(Map<String,Value> metadata )
    {
        Value notificationsValue = metadata.get( "notifications" );
        if ( notificationsValue != null )
        {
            return notificationsValue.asList( InternalNotification.VALUE_TO_NOTIFICATION );
        }
        return Collections.emptyList();
    }

    private static long extractResultConsumedAfter(Map<String,Value> metadata, String key )
    {
        Value resultConsumedAfterValue = metadata.get( key );
        if ( resultConsumedAfterValue != null )
        {
            return resultConsumedAfterValue.asLong();
        }
        return -1;
    }
}
