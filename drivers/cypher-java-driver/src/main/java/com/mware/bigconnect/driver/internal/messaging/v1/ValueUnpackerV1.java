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

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.exceptions.ClientException;
import com.mware.bigconnect.driver.internal.InternalNode;
import com.mware.bigconnect.driver.internal.InternalPath;
import com.mware.bigconnect.driver.internal.InternalRelationship;
import com.mware.bigconnect.driver.internal.messaging.ValueUnpacker;
import com.mware.bigconnect.driver.internal.packstream.PackInput;
import com.mware.bigconnect.driver.internal.packstream.PackStream;
import com.mware.bigconnect.driver.internal.packstream.PackType;
import com.mware.bigconnect.driver.internal.types.TypeConstructor;
import com.mware.bigconnect.driver.internal.util.Iterables;
import com.mware.bigconnect.driver.internal.value.*;
import com.mware.bigconnect.driver.types.Node;
import com.mware.bigconnect.driver.types.Path;
import com.mware.bigconnect.driver.types.Relationship;

import java.io.IOException;
import java.util.*;

import static com.mware.bigconnect.driver.Values.value;

public class ValueUnpackerV1 implements ValueUnpacker
{
    protected final PackStream.Unpacker unpacker;

    public ValueUnpackerV1( PackInput input )
    {
        this.unpacker = new PackStream.Unpacker( input );
    }

    @Override
    public long unpackStructHeader() throws IOException
    {
        return unpacker.unpackStructHeader();
    }

    @Override
    public int unpackStructSignature() throws IOException
    {
        return unpacker.unpackStructSignature();
    }

    @Override
    public Map<String,Value> unpackMap() throws IOException
    {
        int size = (int) unpacker.unpackMapHeader();
        if ( size == 0 )
        {
            return Collections.emptyMap();
        }
        Map<String,Value> map = Iterables.newHashMapWithSize( size );
        for ( int i = 0; i < size; i++ )
        {
            String key = unpacker.unpackString();
            map.put( key, unpack() );
        }
        return map;
    }

    @Override
    public Value[] unpackArray() throws IOException
    {
        int size = (int) unpacker.unpackListHeader();
        Value[] values = new Value[size];
        for ( int i = 0; i < size; i++ )
        {
            values[i] = unpack();
        }
        return values;
    }

    private Value unpack() throws IOException
    {
        PackType type = unpacker.peekNextType();
        switch ( type )
        {
        case NULL:
            return value( unpacker.unpackNull() );
        case BOOLEAN:
            return value( unpacker.unpackBoolean() );
        case INTEGER:
            return value( unpacker.unpackLong() );
        case FLOAT:
            return value( unpacker.unpackDouble() );
        case BYTES:
            return value( unpacker.unpackBytes() );
        case STRING:
            return value( unpacker.unpackString() );
        case MAP:
        {
            return new MapValue( unpackMap() );
        }
        case LIST:
        {
            int size = (int) unpacker.unpackListHeader();
            Value[] vals = new Value[size];
            for ( int j = 0; j < size; j++ )
            {
                vals[j] = unpack();
            }
            return new ListValue( vals );
        }
        case STRUCT:
        {
            long size = unpacker.unpackStructHeader();
            byte structType = unpacker.unpackStructSignature();
            return unpackStruct( size, structType );
        }
        }
        throw new IOException( "Unknown value type: " + type );
    }

    protected Value unpackStruct( long size, byte type ) throws IOException
    {
        switch ( type )
        {
        case MessageFormatV1.NODE:
            ensureCorrectStructSize( TypeConstructor.NODE, MessageFormatV1.NODE_FIELDS, size );
            InternalNode adapted = unpackNode();
            return new NodeValue( adapted );
        case MessageFormatV1.RELATIONSHIP:
            ensureCorrectStructSize( TypeConstructor.RELATIONSHIP, 5, size );
            return unpackRelationship();
        case MessageFormatV1.PATH:
            ensureCorrectStructSize( TypeConstructor.PATH, 3, size );
            return unpackPath();
        default:
            throw new IOException( "Unknown struct type: " + type );
        }
    }

    private Value unpackRelationship() throws IOException
    {
        String urn = unpacker.unpackString();
        String startUrn = unpacker.unpackString();
        String endUrn = unpacker.unpackString();
        String relType = unpacker.unpackString();
        Map<String,Value> props = unpackMap();

        InternalRelationship adapted = new InternalRelationship( urn, startUrn, endUrn, relType, props );
        return new RelationshipValue( adapted );
    }

    private InternalNode unpackNode() throws IOException
    {
        String urn = unpacker.unpackString();

        int numLabels = (int) unpacker.unpackListHeader();
        List<String> labels = new ArrayList<>( numLabels );
        for ( int i = 0; i < numLabels; i++ )
        {
            labels.add( unpacker.unpackString() );
        }
        int numProps = (int) unpacker.unpackMapHeader();
        Map<String,Value> props = Iterables.newHashMapWithSize( numProps );
        for ( int j = 0; j < numProps; j++ )
        {
            String key = unpacker.unpackString();
            props.put( key, unpack() );
        }

        return new InternalNode( urn, labels, props );
    }

    private Value unpackPath() throws IOException
    {
        // List of unique nodes
        Node[] uniqNodes = new Node[(int) unpacker.unpackListHeader()];
        for ( int i = 0; i < uniqNodes.length; i++ )
        {
            ensureCorrectStructSize( TypeConstructor.NODE, MessageFormatV1.NODE_FIELDS, unpacker.unpackStructHeader() );
            ensureCorrectStructSignature( "NODE", MessageFormatV1.NODE, unpacker.unpackStructSignature() );
            uniqNodes[i] = unpackNode();
        }

        // List of unique relationships, without start/end information
        InternalRelationship[] uniqRels = new InternalRelationship[(int) unpacker.unpackListHeader()];
        for ( int i = 0; i < uniqRels.length; i++ )
        {
            ensureCorrectStructSize( TypeConstructor.RELATIONSHIP, 3, unpacker.unpackStructHeader() );
            ensureCorrectStructSignature( "UNBOUND_RELATIONSHIP", MessageFormatV1.UNBOUND_RELATIONSHIP, unpacker.unpackStructSignature() );
            String id = unpacker.unpackString();
            String relType = unpacker.unpackString();
            Map<String,Value> props = unpackMap();
            uniqRels[i] = new InternalRelationship( id, null, null, relType, props );
        }

        // Path sequence
        int length = (int) unpacker.unpackListHeader();

        // Knowing the sequence length, we can create the arrays that will represent the nodes, rels and segments in their "path order"
        Path.Segment[] segments = new Path.Segment[length / 2];
        Node[] nodes = new Node[segments.length + 1];
        Relationship[] rels = new Relationship[segments.length];

        Node prevNode = uniqNodes[0], nextNode; // Start node is always 0, and isn't encoded in the sequence
        nodes[0] = prevNode;
        InternalRelationship rel;
        for ( int i = 0; i < segments.length; i++ )
        {
            int relIdx = (int) unpacker.unpackLong();
            nextNode = uniqNodes[(int) unpacker.unpackLong()];
            // Negative rel index means this rel was traversed "inversed" from its direction
            if ( relIdx < 0 )
            {
                rel = uniqRels[(-relIdx) - 1]; // -1 because rel idx are 1-indexed
                rel.setStartAndEnd( nextNode.id(), prevNode.id() );
            }
            else
            {
                rel = uniqRels[relIdx - 1];
                rel.setStartAndEnd( prevNode.id(), nextNode.id() );
            }

            nodes[i + 1] = nextNode;
            rels[i] = rel;
            segments[i] = new InternalPath.SelfContainedSegment( prevNode, rel, nextNode );
            prevNode = nextNode;
        }
        return new PathValue( new InternalPath( Arrays.asList( segments ), Arrays.asList( nodes ), Arrays.asList( rels ) ) );
    }

    protected final void ensureCorrectStructSize( TypeConstructor typeConstructor, int expected, long actual )
    {
        if ( expected != actual )
        {
            String structName = typeConstructor.toString();
            throw new ClientException( String.format(
                    "Invalid message received, serialized %s structures should have %d fields, "
                    + "received %s structure has %d fields.", structName, expected, structName, actual ) );
        }
    }

    private void ensureCorrectStructSignature(String structName, byte expected, byte actual )
    {
        if ( expected != actual )
        {
            throw new ClientException( String.format(
                    "Invalid message received, expected a `%s`, signature 0x%s. Received signature was 0x%s.",
                    structName, Integer.toHexString( expected ), Integer.toHexString( actual ) ) );
        }
    }
}
