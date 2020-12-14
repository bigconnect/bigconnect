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
package com.mware.bolt.messaging;

import com.mware.ge.values.AnyValue;
import com.mware.ge.values.virtual.MapValue;

import java.io.IOException;

/**
 * Represents a single Bolt message format by exposing a {@link Packer packer} and {@link Unpacker unpacker}
 * for primitives of this format.
 */
public interface BigConnectPack extends PackProvider, UnpackerProvider {
    interface Packer {
        void pack(String value) throws IOException;

        void pack(AnyValue value) throws IOException;

        void packStructHeader(int size, byte signature) throws IOException;

        void packMapHeader(int size) throws IOException;

        void packListHeader(int size) throws IOException;

        void flush() throws IOException;
    }

    interface Unpacker {
        AnyValue unpack() throws IOException;

        String unpackString() throws IOException;

        MapValue unpackMap() throws IOException;

        long unpackStructHeader() throws IOException;

        char unpackStructSignature() throws IOException;

        long unpackListHeader() throws IOException;
    }

    long version();
}
