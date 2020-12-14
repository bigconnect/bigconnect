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
package com.mware.bigconnect.driver.internal.handlers;

import com.mware.bigconnect.driver.Record;
import com.mware.bigconnect.driver.internal.spi.ResponseHandler;
import com.mware.bigconnect.driver.summary.ResultSummary;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public interface PullAllResponseHandler extends ResponseHandler
{
    CompletionStage<ResultSummary> summaryAsync();

    CompletionStage<Record> nextAsync();

    CompletionStage<Record> peekAsync();

    CompletionStage<ResultSummary> consumeAsync();

    <T> CompletionStage<List<T>> listAsync(Function<Record, T> mapFunction);

    CompletionStage<Throwable> failureAsync();
}
