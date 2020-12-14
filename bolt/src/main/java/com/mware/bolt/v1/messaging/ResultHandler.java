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
package com.mware.bolt.v1.messaging;

import com.mware.bolt.messaging.BoltResponseMessageWriter;
import com.mware.bolt.runtime.BoltConnection;
import com.mware.bolt.runtime.BoltResult;
import com.mware.bolt.v1.messaging.response.RecordMessage;
import com.mware.ge.cypher.result.QueryResult;
import com.mware.ge.values.AnyValue;

public class ResultHandler extends MessageProcessingHandler {
    public ResultHandler(BoltResponseMessageWriter handler, BoltConnection connection) {
        super(handler, connection);
    }

    @Override
    public void onRecords(final BoltResult result, final boolean pull) throws Exception {
        result.accept(new BoltResult.Visitor() {
            @Override
            public void visit(QueryResult.Record record) throws Exception {
                if (pull) {
                    messageWriter.write(new RecordMessage(record));
                }
            }

            @Override
            public void addMetadata(String key, AnyValue value) {
                onMetadata(key, value);
            }
        });
    }
}
