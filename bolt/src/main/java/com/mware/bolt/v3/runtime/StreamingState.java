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

import com.mware.bolt.runtime.BoltStateMachineState;
import com.mware.bolt.runtime.StateMachineContext;
import com.mware.bolt.v1.bookmarking.Bookmark;

/**
 * When STREAMING, additionally attach bookmark to PULL_ALL, DISCARD_ALL result
 */
public class StreamingState extends AbstractStreamingState {
    @Override
    public String name() {
        return "STREAMING";
    }

    @Override
    protected BoltStateMachineState processStreamResultMessage(boolean pull, StateMachineContext context) throws Throwable {
        Bookmark bookmark = context.connectionState().getStatementProcessor().streamResult(
                recordStream -> context.connectionState().getResponseHandler().onRecords(recordStream, pull));
        bookmark.attachTo(context.connectionState());
        return readyState;
    }
}
