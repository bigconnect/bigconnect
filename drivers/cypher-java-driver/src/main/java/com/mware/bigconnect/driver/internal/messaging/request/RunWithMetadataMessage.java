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
import com.mware.bigconnect.driver.Statement;
import com.mware.bigconnect.driver.TransactionConfig;
import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.internal.InternalBookmark;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static com.mware.bigconnect.driver.Values.ofValue;
import static com.mware.bigconnect.driver.internal.messaging.request.TransactionMetadataBuilder.buildMetadata;

public class RunWithMetadataMessage extends MessageWithMetadata
{
    public final static byte SIGNATURE = 0x10;

    private final String statement;
    private final Map<String,Value> parameters;

    public static RunWithMetadataMessage autoCommitTxRunMessage(Statement statement, TransactionConfig config, String databaseName, AccessMode mode,
                                                                InternalBookmark bookmark )
    {
        return autoCommitTxRunMessage( statement, config.timeout(), config.metadata(), databaseName, mode, bookmark );
    }

    public static RunWithMetadataMessage autoCommitTxRunMessage(Statement statement, Duration txTimeout, Map<String,Value> txMetadata, String databaseName,
                                                                AccessMode mode, InternalBookmark bookmark )
    {
        Map<String,Value> metadata = buildMetadata( txTimeout, txMetadata, databaseName, mode, bookmark );
        return new RunWithMetadataMessage( statement.text(), statement.parameters().asMap( ofValue() ), metadata );
    }

    public static RunWithMetadataMessage explicitTxRunMessage( Statement statement )
    {
        return new RunWithMetadataMessage( statement.text(), statement.parameters().asMap( ofValue() ), emptyMap() );
    }

    private RunWithMetadataMessage(String statement, Map<String,Value> parameters, Map<String,Value> metadata )
    {
        super( metadata );
        this.statement = statement;
        this.parameters = parameters;
    }

    public String statement()
    {
        return statement;
    }

    public Map<String,Value> parameters()
    {
        return parameters;
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
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
        RunWithMetadataMessage that = (RunWithMetadataMessage) o;
        return Objects.equals( statement, that.statement ) && Objects.equals( parameters, that.parameters ) && Objects.equals( metadata(), that.metadata() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( statement, parameters, metadata() );
    }

    @Override
    public String toString()
    {
        return "RUN \"" + statement + "\" " + parameters + " " + metadata();
    }
}
