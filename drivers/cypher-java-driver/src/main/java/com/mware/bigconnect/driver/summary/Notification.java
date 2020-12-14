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
package com.mware.bigconnect.driver.summary;

import com.mware.bigconnect.driver.util.Immutable;

/**
 * Representation for notifications found when executing a statement.
 *
 * A notification can be visualized in a client pinpointing problems or other information about the statement.
 * @since 1.0
 */
@Immutable
public interface Notification
{
    /**
     * Returns a notification code for the discovered issue.
     * @return the notification code
     */
    String code();

    /**
     * Returns a short summary of the notification.
     * @return the title of the notification.
     */
    String title();

    /**
     * Returns a longer description of the notification.
     * @return the description of the notification.
     */
    String description();

    /**
     * The position in the statement where this notification points to.
     * Not all notifications have a unique position to point to and in that case the position would be set to null.
     *
     * @return the position in the statement where the issue was found, or null if no position is associated with this
     * notification.
     */
    InputPosition position();

    /**
     * The severity level of the notification.
     *
     * @return the severity level of the notification
     */
    String severity();
}
