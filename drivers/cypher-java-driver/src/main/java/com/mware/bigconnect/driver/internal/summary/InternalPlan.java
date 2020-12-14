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
package com.mware.bigconnect.driver.internal.summary;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.Values;
import com.mware.bigconnect.driver.summary.Plan;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.lang.String.format;
import static com.mware.bigconnect.driver.Values.ofString;

public class InternalPlan<T extends Plan> implements Plan
{
    private final String operatorType;
    private final List<String> identifiers;
    private final Map<String, Value> arguments;
    private final List<T> children;

    // Only call when sub-classing, for constructing plans, use .plan instead
    protected InternalPlan(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<T> children )
    {
        this.operatorType = operatorType;
        this.identifiers = identifiers;
        this.arguments = arguments;
        this.children = children;
    }

    @Override
    public String operatorType()
    {
        return operatorType;
    }

    @Override
    public List<String> identifiers()
    {
        return identifiers;
    }

    @Override
    public Map<String, Value> arguments()
    {
        return arguments;
    }

    @Override
    public List<T> children()
    {
        return children;
    }

    @Override
    public String toString()
    {
        return format(
            "SimplePlanTreeNode{operatorType='%s', arguments=%s, identifiers=%s, children=%s}",
            operatorType, arguments, identifiers, children
        );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InternalPlan that = (InternalPlan) o;

        return operatorType.equals( that.operatorType )
            && arguments.equals( that.arguments )
            && identifiers.equals( that.identifiers )
            && children.equals( that.children );
    }

    @Override
    public int hashCode()
    {
        int result = operatorType.hashCode();
        result = 31 * result + identifiers.hashCode();
        result = 31 * result + arguments.hashCode();
        result = 31 * result + children.hashCode();
        return result;
    }

    public static Plan plan(
            String operatorType,
            Map<String, Value> arguments,
            List<String> identifiers,
            List<Plan> children )
    {
        return EXPLAIN_PLAN.create( operatorType, arguments, identifiers, children, null );
    }

    public static final PlanCreator<Plan> EXPLAIN_PLAN = new PlanCreator<Plan>()
    {
        @Override
        public Plan create(String operatorType, Map<String,Value> arguments, List<String> identifiers, List<Plan> children, Value originalPlanValue )
        {
            return new InternalPlan<>( operatorType, arguments, identifiers, children );
        }
    };

    /** Builds a regular plan without profiling information - eg. a plan that came as a result of an `EXPLAIN` statement */
    public static final Function<Value, Plan> EXPLAIN_PLAN_FROM_VALUE = new Converter<>(EXPLAIN_PLAN);

    /**
     * Since a plan with or without profiling looks almost the same, we just keep two impls. of this
     * around to contain the small difference, and share the rest of the code for building plan trees.
     * @param <T>
     */
    interface PlanCreator<T extends Plan>
    {
        T create(String operatorType,
                 Map<String, Value> arguments,
                 List<String> identifiers,
                 List<T> children,
                 Value originalPlanValue);
    }

    static class Converter<T extends Plan> implements Function<Value, T>
    {
        private final PlanCreator<T> planCreator;

        public Converter( PlanCreator<T> planCreator )
        {
            this.planCreator = planCreator;
        }

        @Override
        public T apply( Value plan )
        {
            final String operatorType = plan.get( "operatorType" ).asString();

            final Value argumentsValue = plan.get( "args" );
            final Map<String, Value> arguments = argumentsValue.isNull()
                    ? Collections.<String, Value>emptyMap()
                    : argumentsValue.asMap( Values.ofValue() );

            final Value identifiersValue = plan.get( "identifiers" );
            final List<String> identifiers = identifiersValue.isNull()
                    ? Collections.<String>emptyList()
                    : identifiersValue.asList( ofString() );

            final Value childrenValue = plan.get( "children" );
            final List<T> children = childrenValue.isNull()
                    ? Collections.<T>emptyList()
                    : childrenValue.asList( this );

            return planCreator.create( operatorType, arguments, identifiers, children, plan );
        }
    }
}

