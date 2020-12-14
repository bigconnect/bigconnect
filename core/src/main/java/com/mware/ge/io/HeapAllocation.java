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
package com.mware.ge.io;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import static java.lang.Character.toUpperCase;
import static java.util.Objects.requireNonNull;

public abstract class HeapAllocation {
    public static final HeapAllocation HEAP_ALLOCATION;
    public static final HeapAllocation NOT_AVAILABLE;

    static {
        NOT_AVAILABLE = new HeapAllocationNotAvailable(); // must be first!
        HEAP_ALLOCATION = load(ManagementFactory.getThreadMXBean());
    }

    /**
     * Returns number of allocated bytes by the thread.
     *
     * @param thread the thread to get the used CPU time for.
     * @return number of allocated bytes for specified thread.
     */
    public final long allocatedBytes(Thread thread) {
        return allocatedBytes(thread.getId());
    }

    /**
     * Returns number of allocated bytes by the thread.
     *
     * @param threadId the id of the thread to get the allocation information for.
     * @return number of allocated bytes for specified threadId.
     */
    public abstract long allocatedBytes(long threadId);

    private static HeapAllocation load(ThreadMXBean bean) {
        Class<HeapAllocation> base = HeapAllocation.class;
        StringBuilder name = new StringBuilder().append(base.getPackage().getName()).append('.');
        String pkg = bean.getClass().getPackage().getName();
        int start = 0;
        int end = pkg.indexOf('.', start);
        while (end > 0) {
            name.append(toUpperCase(pkg.charAt(start))).append(pkg, start + 1, end);
            start = end + 1;
            end = pkg.indexOf('.', start);
        }
        name.append(toUpperCase(pkg.charAt(start))).append(pkg.substring(start + 1));
        name.append(base.getSimpleName());
        try {
            return requireNonNull((HeapAllocation) Class.forName(name.toString())
                    .getDeclaredMethod("load", ThreadMXBean.class)
                    .invoke(null, bean), "Loader method returned null.");
        } catch (Throwable e) {
            //noinspection ConstantConditions -- this can actually happen if the code order is wrong
            if (NOT_AVAILABLE == null) {
                throw new LinkageError("Bad code loading order.", e);
            }
            return NOT_AVAILABLE;
        }
    }

    private static class HeapAllocationNotAvailable extends HeapAllocation {
        @Override
        public long allocatedBytes(long threadId) {
            return -1;
        }
    }
}
