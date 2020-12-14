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
package com.mware.bigconnect.driver.internal.types;

import com.mware.bigconnect.driver.Value;
import com.mware.bigconnect.driver.types.Type;
import com.mware.bigconnect.driver.types.TypeSystem;

import static com.mware.bigconnect.driver.internal.types.TypeConstructor.*;

/**
 * Utility class for determining and working with the Cypher types of values
 *
 * @see Value
 * @see Type
 */
public class InternalTypeSystem implements TypeSystem
{
    public static InternalTypeSystem TYPE_SYSTEM = new InternalTypeSystem();

    private final TypeRepresentation anyType = constructType( ANY );
    private final TypeRepresentation booleanType = constructType( BOOLEAN );
    private final TypeRepresentation bytesType = constructType( BYTES );
    private final TypeRepresentation stringType = constructType( STRING );
    private final TypeRepresentation numberType = constructType( NUMBER );
    private final TypeRepresentation integerType = constructType( INTEGER );
    private final TypeRepresentation floatType = constructType( FLOAT );
    private final TypeRepresentation listType = constructType( LIST );
    private final TypeRepresentation mapType = constructType( MAP );
    private final TypeRepresentation nodeType = constructType( NODE );
    private final TypeRepresentation relationshipType = constructType( RELATIONSHIP );
    private final TypeRepresentation pathType = constructType( PATH );
    private final TypeRepresentation pointType = constructType( POINT );
    private final TypeRepresentation dateType = constructType( DATE );
    private final TypeRepresentation timeType = constructType( TIME );
    private final TypeRepresentation localTimeType = constructType( LOCAL_TIME );
    private final TypeRepresentation localDateTimeType = constructType( LOCAL_DATE_TIME );
    private final TypeRepresentation dateTimeType = constructType( DATE_TIME );
    private final TypeRepresentation durationType = constructType( DURATION );
    private final TypeRepresentation nullType = constructType( NULL );

    private InternalTypeSystem()
    {
    }

    @Override
    public Type ANY()
    {
        return anyType;
    }

    @Override
    public Type BOOLEAN()
    {
        return booleanType;
    }

    @Override
    public Type BYTES()
    {
        return bytesType;
    }

    @Override
    public Type STRING()
    {
        return stringType;
    }

    @Override
    public Type NUMBER()
    {
        return numberType;
    }

    @Override
    public Type INTEGER()
    {
        return integerType;
    }

    @Override
    public Type FLOAT()
    {
        return floatType;
    }

    @Override
    public Type LIST()
    {
        return listType;
    }

    @Override
    public Type MAP()
    {
        return mapType;
    }

    @Override
    public Type NODE()
    {
        return nodeType;
    }

    @Override
    public Type RELATIONSHIP()
    {
        return relationshipType;
    }

    @Override
    public Type PATH()
    {
        return pathType;
    }

    @Override
    public Type POINT()
    {
        return pointType;
    }

    @Override
    public Type DATE()
    {
        return dateType;
    }

    @Override
    public Type TIME()
    {
        return timeType;
    }

    @Override
    public Type LOCAL_TIME()
    {
        return localTimeType;
    }

    @Override
    public Type LOCAL_DATE_TIME()
    {
        return localDateTimeType;
    }

    @Override
    public Type DATE_TIME()
    {
        return dateTimeType;
    }

    @Override
    public Type DURATION()
    {
        return durationType;
    }

    @Override
    public Type NULL()
    {
        return nullType;
    }

    private TypeRepresentation constructType( TypeConstructor tyCon )
    {
        return new TypeRepresentation( tyCon );
    }
}
