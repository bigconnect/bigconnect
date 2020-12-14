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
package com.mware.bigconnect.driver.internal.handlers.pulln;

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;
import com.mware.bigconnect.driver.summary.ResultSummary;
import org.reactivestreams.Subscription;

import java.util.function.BiConsumer;

public interface BasicPullResponseHandler extends ResponseHandler, Subscription
{
    /**
     * Register a record consumer for each record received.
     * STREAMING shall not be started before this consumer is registered.
     * A null record with no error indicates the end of streaming.
     * @param recordConsumer register a record consumer to be notified for each record received.
     */
    void installRecordConsumer(BiConsumer<Record, Throwable> recordConsumer);

    /**
     * Register a summary consumer to be notified when a summary is received.
     * STREAMING shall not be started before this consumer is registered.
     * A null summary with no error indicates a SUCCESS message with has_more=true has arrived.
     * @param summaryConsumer register a summary consumer
     */
    void installSummaryConsumer(BiConsumer<ResultSummary, Throwable> summaryConsumer);

    enum Status
    {
        DONE,       // successfully completed
        FAILED,     // failed
        CANCELED,   // canceled
        STREAMING,  // streaming records
        READY       // steaming is paused. ready to accept request or cancel commands from user
    }
}
