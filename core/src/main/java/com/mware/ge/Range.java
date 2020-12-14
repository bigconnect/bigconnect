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
package com.mware.ge;

import com.mware.ge.util.ObjectUtils;
import com.mware.ge.util.StringUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Objects;

public class Range<T> implements Serializable {
    private static final long serialVersionUID = -4491252292678133754L;
    private T start;
    private final boolean inclusiveStart;
    private T end;
    private final boolean inclusiveEnd;
    private String prefix;

    protected Range() {
        this.start = null;
        this.end = null;
        this.inclusiveStart = false;
        this.inclusiveEnd = false;
    }

    /**
     * Creates a range object.
     *
     * @param start          The start value. null if the start of all.
     * @param inclusiveStart true, if the start value should be included.
     * @param end            The end value. null if the end of all.
     * @param inclusiveEnd   true, if the end value should be included.
     */
    public Range(T start, boolean inclusiveStart, T end, boolean inclusiveEnd) {
        this.start = start;
        this.inclusiveStart = inclusiveStart;
        this.end = end;
        this.inclusiveEnd = inclusiveEnd;
    }

    public Range(T row) {
        this(row, true, row, true);
    }

    public T getStart() {
        return start;
    }
    public boolean isInclusiveStart() {
        return inclusiveStart;
    }

    public T getEnd() {
        return end;
    }

    public boolean isInclusiveEnd() {
        return inclusiveEnd;
    }

    public boolean isInRange(Object obj) {
        if (getStart() != null) {
            if (isInclusiveStart()) {
                if (ObjectUtils.compare(getStart(), obj) > 0) {
                    return false;
                }
            } else {
                if (ObjectUtils.compare(getStart(), obj) >= 0) {
                    return false;
                }
            }
        }

        if (getEnd() != null) {
            if (isInclusiveEnd()) {
                if (ObjectUtils.compare(obj, getEnd()) > 0) {
                    return false;
                }
            } else {
                if (ObjectUtils.compare(obj, getEnd()) >= 0) {
                    return false;
                }
            }
        }

        return true;
    }

    public static IdRange prefix(String prefix) {
        IdRange r = new IdRange();
        r.setPrefix(prefix);
        return r;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }

    @Override
    public String toString() {
        return String.format(
                "%s{start=%s, inclusiveStart=%s, end=%s, inclusiveEnd=%s}",
                this.getClass().getSimpleName(),
                start,
                inclusiveStart,
                end,
                inclusiveEnd
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Range)) {
            return false;
        }

        Range range = (Range) o;

        return inclusiveStart == range.inclusiveStart
                && inclusiveEnd == range.inclusiveEnd
                && Objects.equals(start, range.start)
                && Objects.equals(end, range.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, inclusiveStart, end, inclusiveEnd);
    }

    public boolean isEmpty() {
        if (start == null && end == null)
            return true;

        if (start instanceof String && end instanceof String) {
            return StringUtils.isEmpty((String) start) &&
                StringUtils.isEmpty((String) end);
        }

        if (start instanceof byte[] && end instanceof byte[]) {
            return ((byte[])start).length == 0 &&
                    ((byte[])end).length == 0;
        }

        assert start != null;

        throw new IllegalStateException("Don't know how to test for emptiness for type: "+start.getClass().getName());
    }
}
