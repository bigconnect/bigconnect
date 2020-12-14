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
package com.mware.bigconnect.driver.internal;

import java.io.Serializable;

/**
 * Causal chaining is carried out by passing bookmarks between transactions.
 *
 * When starting a session with initial bookmarks, the first transaction will be ensured to run at least after
 * the database is as up-to-date as the latest transaction referenced by the supplied bookmarks.
 *
 * Within a session, bookmark propagation is carried out automatically.
 * Thus all transactions in a session including explicit and implicit transactions are ensured to be carried out one after another.
 *
 * To opt out of this mechanism for unrelated units of work, applications can use multiple sessions.
 */
public interface Bookmark extends Serializable
{
}
