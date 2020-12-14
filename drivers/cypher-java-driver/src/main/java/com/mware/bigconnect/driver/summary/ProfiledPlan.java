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

import java.util.List;

/**
 * This is the same as a regular {@link Plan} - except this plan has been executed, meaning it also contains detailed information about how much work each
 * step of the plan incurred on the database.
 * @since 1.0
 */
public interface ProfiledPlan extends Plan
{
    /**
     * @return the number of times this part of the plan touched the underlying data stores
     */
    long dbHits();

    /**
     * @return the number of records this part of the plan produced
     */
    long records();

    @Override
    List<ProfiledPlan> children();
}
