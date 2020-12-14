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

import com.mware.bolt.messaging.*;
import com.mware.bolt.runtime.Status;
import com.mware.bolt.v1.messaging.encoder.FailureMessageEncoder;
import com.mware.bolt.v1.messaging.encoder.IgnoredMessageEncoder;
import com.mware.bolt.v1.messaging.encoder.RecordMessageEncoder;
import com.mware.bolt.v1.messaging.encoder.SuccessMessageEncoder;
import com.mware.bolt.v1.messaging.response.*;
import com.mware.bolt.v1.packstream.PackOutput;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Writer for Bolt request messages to be sent to a {@link BigConnectPack.Packer}.
 */
public class BoltResponseMessageWriterV1 implements BoltResponseMessageWriter {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(BoltResponseMessageWriterV1.class);

    private final PackOutput output;
    private final BigConnectPack.Packer packer;
    private final Map<Byte, ResponseMessageEncoder<ResponseMessage>> encoders;

    public BoltResponseMessageWriterV1(PackProvider packerProvider, PackOutput output) {
        this.output = output;
        this.packer = packerProvider.newPacker(output);
        this.encoders = registerEncoders();
    }

    private Map<Byte, ResponseMessageEncoder<ResponseMessage>> registerEncoders() {
        Map<Byte, ResponseMessageEncoder<?>> encoders = new HashMap<>();
        encoders.put(SuccessMessage.SIGNATURE, new SuccessMessageEncoder());
        encoders.put(RecordMessage.SIGNATURE, new RecordMessageEncoder());
        encoders.put(IgnoredMessage.SIGNATURE, new IgnoredMessageEncoder());
        encoders.put(FailureMessage.SIGNATURE, new FailureMessageEncoder());
        return (Map) encoders;
    }

    @Override
    public void write(ResponseMessage message) throws IOException {
        packCompleteMessageOrFail(message);
        if (message instanceof FatalFailureMessage) {
            flush();
        }
    }

    public void flush() throws IOException {
        packer.flush();
    }

    private void packCompleteMessageOrFail(ResponseMessage message) throws IOException {
        boolean packingFailed = true;
        output.beginMessage();
        try {
            ResponseMessageEncoder<ResponseMessage> encoder = encoders.get(message.signature());
            if (encoder == null) {
                throw new BoltIOException(Status.Request.InvalidFormat,
                        String.format("Message %s is not supported in this protocol version.", message));
            }
            encoder.encode(packer, message);
            packingFailed = false;
            output.messageSucceeded();
        } catch (Throwable error) {
            if (packingFailed) {
                // packing failed, there might be some half-written data in the output buffer right now
                // notify output about the failure so that it cleans up the buffer
                output.messageFailed();
                LOGGER.error("Failed to write full %s message because: %s", message, error.getMessage());
            }
            throw error;
        }
    }
}

