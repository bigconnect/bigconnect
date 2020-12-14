/*
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
package com.mware.bolt.util;

import java.util.concurrent.ThreadFactory;

public class BoltThreadFactory implements ThreadFactory {
    private final ThreadGroup threadGroup;

    public BoltThreadFactory(String group) {
        threadGroup = new ThreadGroup( group );
    }

    @Override
    public Thread newThread(Runnable job) {
        Thread thread = new Thread(threadGroup, job, "bolt"+System.nanoTime()) {
            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder( "Thread[" ).append( getName() );
                ThreadGroup group = getThreadGroup();
                String sep = ", in ";
                while ( group != null )
                {
                    sb.append( sep ).append( group.getName() );
                    group = group.getParent();
                    sep = "/";
                }
                return sb.append( ']' ).toString();
            }
        };
        thread.setDaemon( true );
        return thread;
    }
}
