/*
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
package com.mware.bolt.v3.runtime;

import com.mware.bolt.v1.runtime.CypherAdapterStream;
import com.mware.ge.cypher.result.QueryResult;
import com.mware.ge.values.storable.Values;

import java.time.Clock;

public class CypherAdapterStreamV3 extends CypherAdapterStream {
    private static final String LAST_RESULT_CONSUMED_KEY = "t_last";

    public CypherAdapterStreamV3(QueryResult result, Clock clock) {
        super(result, clock);
    }

    @Override
    protected void addRecordStreamingTime(Visitor visitor, long time) {
        visitor.addMetadata(LAST_RESULT_CONSUMED_KEY, Values.longValue(time));
    }
}
