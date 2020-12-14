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
package com.mware.bigconnect.driver.util;

/**
 * A Resource is an {@link AutoCloseable} that allows introspecting if it
 * already has been closed through its {@link #isOpen()} method.
 *
 * Additionally, calling {@link AutoCloseable#close()} twice is expected to fail
 * (i.e. is not idempotent).
 * @since 1.0
 */
public interface Resource extends AutoCloseable
{
    /**
     * Detect whether this resource is still open
     *
     * @return true if the resource is open
     */
    boolean isOpen();

    /**
     * @throws IllegalStateException if already closed
     */
    @Override
    void close();
}
