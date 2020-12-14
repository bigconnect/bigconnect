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

import com.mware.bolt.messaging.RequestMessage;
import com.mware.bolt.runtime.BoltStateMachineState;
import com.mware.bolt.runtime.StateMachineContext;
import com.mware.bolt.runtime.StatementMetadata;
import com.mware.bolt.runtime.StatementProcessor;
import com.mware.bolt.v1.bookmarking.Bookmark;
import com.mware.bolt.v3.messaging.request.CommitMessage;
import com.mware.bolt.v3.messaging.request.RollbackMessage;
import com.mware.bolt.v3.messaging.request.RunMessage;
import com.mware.core.exception.BcException;
import com.mware.ge.values.storable.Values;

import static com.mware.bolt.v3.runtime.ReadyState.FIELDS_KEY;
import static com.mware.bolt.v3.runtime.ReadyState.FIRST_RECORD_AVAILABLE_KEY;
import static com.mware.ge.util.Preconditions.checkState;

public class TransactionReadyState extends FailSafeBoltStateMachineState {
    private BoltStateMachineState streamingState;
    private BoltStateMachineState readyState;

    @Override
    public BoltStateMachineState processUnsafe(RequestMessage message, StateMachineContext context) throws Exception {
        if (message instanceof RunMessage) {
            return processRunMessage((RunMessage) message, context);
        }
        if (message instanceof CommitMessage) {
            return processCommitMessage(context);
        }
        if (message instanceof RollbackMessage) {
            return processRollbackMessage(context);
        }
        return null;
    }

    @Override
    public String name() {
        return "TX_READY";
    }

    public void setTransactionStreamingState(BoltStateMachineState streamingState) {
        this.streamingState = streamingState;
    }

    public void setReadyState(BoltStateMachineState readyState) {
        this.readyState = readyState;
    }

    private BoltStateMachineState processRunMessage(RunMessage message, StateMachineContext context) throws BcException {
        long start = context.clock().millis();
        StatementProcessor statementProcessor = context.connectionState().getStatementProcessor();
        StatementMetadata statementMetadata = statementProcessor.run(message.statement(), message.params());
        long end = context.clock().millis();

        context.connectionState().onMetadata(FIELDS_KEY, Values.stringArray(statementMetadata.fieldNames()));
        context.connectionState().onMetadata(FIRST_RECORD_AVAILABLE_KEY, Values.longValue(end - start));
        return streamingState;
    }

    private BoltStateMachineState processCommitMessage(StateMachineContext context) throws Exception {
        StatementProcessor statementProcessor = context.connectionState().getStatementProcessor();
        Bookmark bookmark = statementProcessor.commitTransaction();
        bookmark.attachTo(context.connectionState());
        return readyState;
    }

    private BoltStateMachineState processRollbackMessage(StateMachineContext context) throws Exception {
        StatementProcessor statementProcessor = context.connectionState().getStatementProcessor();
        statementProcessor.rollbackTransaction();
        return readyState;
    }

    @Override
    protected void assertInitialized() {
        checkState(streamingState != null, "Streaming state not set");
        checkState(readyState != null, "Ready state not set");
        super.assertInitialized();
    }
}
