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

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.util.Immutable;

import java.util.List;
import java.util.Map;

/**
 * This describes the plan that the database planner produced and used (or will use) to execute your statement.
 * This can be extremely helpful in understanding what a statement is doing, and how to optimize it. For more
 * details, see the BigConnect Manual.
 *
 * The plan for the statement is a tree of plans - each sub-tree containing zero or more child plans. The statement
 * starts with the root plan. Each sub-plan is of a specific {@link #operatorType() operator type}, which describes
 * what that part of the plan does - for instance, perform an index lookup or filter results. The BigConnect Manual contains
 * a reference of the available operator types, and these may differ across BigConnect versions.
 *
 * For a simple view of a plan, the {@code toString} method will give a human-readable rendering of the tree.
 * @since 1.0
 */
@Immutable
public interface Plan
{
    /**
     * @return the operation this plan is performing.
     */
    String operatorType();

    /**
     * Many {@link #operatorType() operators} have arguments defining their specific behavior. This map contains
     * those arguments.
     *
     * @return the arguments for the {@link #operatorType() operator} used.
     */
    Map<String,Value> arguments();

    /**
     * Identifiers used by this part of the plan. These can be both identifiers introduce by you, or automatically
     * generated identifiers.
     * @return a list of identifiers used by this plan.
     */
    List<String> identifiers();

    /**
     * As noted in the class-level javadoc, a plan is a tree, where each child is another plan. The children are where
     * this part of the plan gets its input records - unless this is an {@link #operatorType() operator} that introduces
     * new records on its own.
     * @return zero or more child plans.
     */
    List<? extends Plan> children();
}
