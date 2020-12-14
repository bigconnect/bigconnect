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
import com.mware.bigconnect.driver.summary.InputPosition;
import com.mware.bigconnect.driver.summary.Notification;

import java.util.function.Function;

import static com.mware.bigconnect.driver.internal.value.NullValue.NULL;

public class InternalNotification implements Notification
{
    public static final Function<Value, Notification> VALUE_TO_NOTIFICATION = new Function<Value,Notification>()
    {
        @Override
        public Notification apply( Value value )
        {
            String code = value.get( "code" ).asString();
            String title = value.get( "title" ).asString();
            String description = value.get( "description" ).asString();
            String severity = value.containsKey( "severity" ) ?
                              value.get( "severity" ).asString()
                              : "N/A";

            Value posValue = value.get( "position" );
            InputPosition position = null;
            if( posValue != NULL )
            {
                position = new InternalInputPosition( posValue.get( "offset" ).asInt(),
                                                    posValue.get( "line" ).asInt(),
                                                    posValue.get( "column" ).asInt() );
            }

            return new InternalNotification( code, title, description, severity, position );
        }
    };

    private final String code;
    private final String title;
    private final String description;
    private final String severity;
    private final InputPosition position;

    public InternalNotification(String code, String title, String description, String severity, InputPosition position )
    {
        this.code = code;
        this.title = title;
        this.description = description;
        this.severity = severity;
        this.position = position;
    }

    @Override
    public String code()
    {
        return code;
    }

    @Override
    public String title()
    {
        return title;
    }

    @Override
    public String description()
    {
        return description;
    }

    @Override
    public InputPosition position()
    {
        return position;
    }

    @Override
    public String severity()
    {
        return severity;
    }

    @Override
    public String toString()
    {
        String info = "code=" + code + ", title=" + title + ", description=" + description + ", severity=" + severity;
        return position == null ? info : info + ", position={" + position + "}";
    }
}
