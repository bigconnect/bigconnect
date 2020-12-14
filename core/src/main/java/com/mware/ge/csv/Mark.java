/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package com.mware.ge.csv;

import static java.lang.String.format;

/**
 * A mutable marker that is changed to hold progress made to a {@link BufferedCharSeeker}.
 * It holds information such as start/end position in the data stream, which character
 * was the match and whether or not this denotes the last value of the line.
 */
public class Mark
{
    public static final int END_OF_LINE_CHARACTER = -1;

    private int startPosition;
    private int position;
    private int character;
    private boolean quoted;

    /**
     * @param startPosition position of first character in value (inclusive).
     * @param position position of last character in value (exclusive).
     * @param character use {@code -1} to denote that the matching character was an end-of-line or end-of-file
     * @param quoted whether or not the original data was quoted.
     */
    void set( int startPosition, int position, int character, boolean quoted )
    {
        this.startPosition = startPosition;
        this.position = position;
        this.character = character;
        this.quoted = quoted;
    }

    public int character()
    {
        assert !isEndOfLine();
        return character;
    }

    public boolean isEndOfLine()
    {
        return character == -1;
    }

    public boolean isQuoted()
    {
        return quoted;
    }

    int position()
    {
        if ( position == -1 )
        {
            throw new IllegalStateException( "No value to extract here" );
        }
        return position;
    }

    int startPosition()
    {
        if ( startPosition == -1 )
        {
            throw new IllegalStateException( "No value to extract here" );
        }
        return startPosition;
    }

    int length()
    {
        return position - startPosition;
    }

    @Override
    public String toString()
    {
        return format( "Mark[from:%d, to:%d, quoted:%b]", startPosition, position, quoted);
    }
}
