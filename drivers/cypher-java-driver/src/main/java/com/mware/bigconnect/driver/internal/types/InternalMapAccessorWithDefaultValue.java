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
import com.mware.bigconnect.driver.Values;
import com.mware.bigconnect.driver.internal.AsValue;
import com.mware.bigconnect.driver.types.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class InternalMapAccessorWithDefaultValue implements MapAccessorWithDefaultValue
{
    public abstract Value get( String key );

    @Override
    public Value get(String key, Value defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Value get( Value value, Value defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return ((AsValue) value).asValue();
        }
    }

    @Override
    public Object get(String key, Object defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Object get(Value value, Object defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asObject();
        }
    }

    @Override
    public Number get(String key, Number defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Number get(Value value, Number defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asNumber();
        }
    }

    @Override
    public Entity get(String key, Entity defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Entity get( Value value, Entity defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asEntity();
        }
    }

    @Override
    public Node get(String key, Node defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Node get( Value value, Node defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asNode();
        }
    }

    @Override
    public Path get(String key, Path defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Path get( Value value, Path defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asPath();
        }
    }

    @Override
    public Relationship get(String key, Relationship defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Relationship get( Value value, Relationship defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asRelationship();
        }
    }

    @Override
    public List<Object> get(String key, List<Object> defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private List<Object> get(Value value, List<Object> defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return  value.asList();
        }
    }

    @Override
    public  <T> List<T> get(String key, List<T> defaultValue, Function<Value,T> mapFunc )
    {
        return get( get( key ), defaultValue, mapFunc );
    }

    private <T> List<T> get(Value value, List<T> defaultValue, Function<Value, T> mapFunc )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asList( mapFunc );
        }
    }

    @Override
    public Map<String, Object> get(String key, Map<String, Object> defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Map<String, Object> get(Value value, Map<String, Object> defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asMap();
        }
    }

    @Override
    public <T> Map<String, T> get(String key, Map<String,T> defaultValue, Function<Value,T> mapFunc )
    {
        return get( get( key ), defaultValue, mapFunc );
    }

    private <T> Map<String, T> get(Value value, Map<String, T> defaultValue, Function<Value, T> mapFunc )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asMap( mapFunc );
        }
    }

    @Override
    public int get(String key, int defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private int get( Value value, int defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asInt();
        }
    }

    @Override
    public long get(String key, long defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private long get( Value value, long defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asLong();
        }
    }

    @Override
    public boolean get(String key, boolean defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private boolean get( Value value, boolean defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asBoolean();
        }
    }

    @Override
    public String get(String key, String defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private String get(Value value, String defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asString();
        }
    }

    @Override
    public float get(String key, float defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private float get( Value value, float defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asFloat();
        }
    }

    @Override
    public double get(String key, double defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private double get( Value value, double defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asDouble();
        }
    }

}
