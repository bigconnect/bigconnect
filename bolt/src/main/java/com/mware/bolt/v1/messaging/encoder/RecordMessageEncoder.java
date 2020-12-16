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
package com.mware.bolt.v1.messaging.encoder;

import com.mware.bolt.messaging.BigConnectPack;
import com.mware.bolt.messaging.ResponseMessageEncoder;
import com.mware.bolt.v1.messaging.response.RecordMessage;
import com.mware.ge.values.AnyValue;

import java.io.IOException;

public class RecordMessageEncoder implements ResponseMessageEncoder<RecordMessage> {
    @Override
    public void encode(BigConnectPack.Packer packer, RecordMessage message) throws IOException {
        AnyValue[] fields = message.fields();
        packer.packStructHeader(1, message.signature());
        packer.packListHeader(fields.length);
        for (AnyValue field : fields) {
            packer.pack(field);
        }
    }
}