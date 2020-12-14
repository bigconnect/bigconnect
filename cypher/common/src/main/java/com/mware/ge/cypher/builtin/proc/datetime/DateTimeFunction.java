/*
 * Copyright (c) 2013-2020 "BigConnect,"
 * MWARE SOLUTIONS SRL
 *
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package com.mware.ge.cypher.builtin.proc.datetime;

import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.cypher.procedure.impl.FieldSignature;
import com.mware.ge.cypher.procedure.impl.Neo4jTypes;
import com.mware.ge.cypher.procedure.impl.QualifiedName;
import com.mware.ge.cypher.procedure.impl.UserFunctionSignature;
import com.mware.ge.cypher.exception.Status;
import com.mware.ge.cypher.procedure.impl.CallableUserFunction;
import com.mware.ge.cypher.procedure.impl.Context;
import com.mware.ge.cypher.procedure.exec.Procedures;
import com.mware.ge.cypher.procedure.Description;
import com.mware.ge.values.AnyValue;
import com.mware.ge.values.storable.DateTimeValue;
import com.mware.ge.values.storable.IntegralValue;
import com.mware.ge.values.storable.TemporalValue;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.virtual.MapValue;

import java.time.Clock;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static com.mware.ge.cypher.procedure.impl.FieldSignature.inputField;
import static com.mware.ge.cypher.procedure.impl.Neo4jTypes.NTDateTime;

@Description( "Create a DateTime instant." )
class DateTimeFunction extends TemporalFunction<DateTimeValue>
{
    DateTimeFunction( Supplier<ZoneId> defaultZone )
    {
        super( NTDateTime, defaultZone );
    }

    @Override
    protected DateTimeValue now(Clock clock, String timezone, Supplier<ZoneId> defaultZone )
    {
        return timezone == null ? DateTimeValue.now( clock, defaultZone ) : DateTimeValue.now( clock, timezone );
    }

    @Override
    protected DateTimeValue parse(TextValue value, Supplier<ZoneId> defaultZone )
    {
        return DateTimeValue.parse( value, defaultZone );
    }

    @Override
    protected DateTimeValue build(MapValue map, Supplier<ZoneId> defaultZone )
    {
        return DateTimeValue.build( map, defaultZone );
    }

    @Override
    protected DateTimeValue select(AnyValue from, Supplier<ZoneId> defaultZone )
    {
        return DateTimeValue.select( from, defaultZone );
    }

    @Override
    protected DateTimeValue truncate(TemporalUnit unit, TemporalValue input, MapValue fields, Supplier<ZoneId> defaultZone )
    {
        return DateTimeValue.truncate( unit, input, fields, defaultZone );
    }

    @Override
    void registerMore( Procedures procedures ) throws ProcedureException
    {
        procedures.register( new FromEpoch() );
        procedures.register( new FromEpochMillis() );
    }

    private static class FromEpoch implements CallableUserFunction
    {
        private static final String DESCRIPTION =
                "Create a DateTime given the seconds and nanoseconds since the start of the epoch.";
        private static final List<FieldSignature> SIGNATURE = Arrays.asList(
                inputField( "seconds", Neo4jTypes.NTNumber ),
                inputField( "nanoseconds", Neo4jTypes.NTNumber ) );
        private final UserFunctionSignature signature;

        private FromEpoch()
        {
            this.signature = new UserFunctionSignature(
                    new QualifiedName( new String[] {"datetime"}, "fromepoch" ),
                    SIGNATURE, Neo4jTypes.NTDateTime, null, new String[0],
                    DESCRIPTION, true );
        }

        @Override
        public UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public AnyValue apply(Context ctx, AnyValue[] input ) throws ProcedureException
        {
            if ( input != null && input.length == 2 )
            {
                if ( input[0] instanceof IntegralValue && input[1] instanceof IntegralValue)
                {
                    IntegralValue seconds = (IntegralValue) input[0];
                    IntegralValue nanoseconds = (IntegralValue) input[1];
                    return DateTimeValue.ofEpoch(seconds, nanoseconds);
                }
            }
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature for " + getClass().getSimpleName() +
                ": Provided input was " + Arrays.toString( input ) );
        }
    }

    private static class FromEpochMillis implements CallableUserFunction
    {
        private static final String DESCRIPTION =
                "Create a DateTime given the milliseconds since the start of the epoch.";
        private static final List<FieldSignature> SIGNATURE = Collections.singletonList( inputField( "milliseconds", Neo4jTypes.NTNumber ) );
        private final UserFunctionSignature signature;

        private FromEpochMillis()
        {
            this.signature = new UserFunctionSignature(
                    new QualifiedName( new String[] {"datetime"}, "fromepochmillis" ),
                    SIGNATURE, Neo4jTypes.NTDateTime, null, new String[0],
                    DESCRIPTION, true );
        }

        @Override
        public UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public AnyValue apply(Context ctx, AnyValue[] input ) throws ProcedureException
        {
            if ( input != null && input.length == 1 )
            {
                if ( input[0] instanceof IntegralValue)
                {
                    IntegralValue milliseconds = (IntegralValue) input[0];
                    return DateTimeValue.ofEpochMillis( milliseconds );
                }
            }
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, "Invalid call signature for " + getClass().getSimpleName() +
                    ": Provided input was " + Arrays.toString( input ) );
        }
    }
}
