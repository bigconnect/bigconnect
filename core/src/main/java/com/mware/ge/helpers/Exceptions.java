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
package com.mware.ge.helpers;

import com.mware.ge.function.Predicates;

import java.lang.reflect.Field;
import java.util.function.Predicate;

public class Exceptions {
    private Exceptions() {
        throw new AssertionError("No instances");
    }

    public static final Thread.UncaughtExceptionHandler SILENT_UNCAUGHT_EXCEPTION_HANDLER = (t, e) ->
    {   // Don't print about it
    };

    /**
     * @deprecated Use {@link Throwable#initCause(Throwable)} instead.
     */
    @Deprecated
    public static <T extends Throwable> T withCause(T exception, Throwable cause) {
        try {
            exception.initCause(cause);
        } catch (Exception failure) {
            // OK, we did our best, guess there will be no cause
        }
        return exception;
    }

    /**
     * @deprecated Use {@link Throwable#addSuppressed(Throwable)} instead.
     */
    @Deprecated
    public static <T extends Throwable> T withSuppressed(T exception, Throwable... suppressed) {
        if (suppressed != null) {
            for (Throwable s : suppressed) {
                exception.addSuppressed(s);
            }
        }
        return exception;
    }

    @SuppressWarnings("rawtypes")
    public static boolean contains(final Throwable cause, final String containsMessage, final Class... anyOfTheseClasses) {
        final Predicate<Throwable> anyOfClasses = Predicates.instanceOfAny(anyOfTheseClasses);
        return contains(cause, item -> item.getMessage() != null && item.getMessage().contains(containsMessage) &&
                anyOfClasses.test(item));
    }

    @SuppressWarnings("rawtypes")
    public static boolean contains(Throwable cause, Class... anyOfTheseClasses) {
        return contains(cause, Predicates.instanceOfAny(anyOfTheseClasses));
    }

    public static boolean contains(Throwable cause, Predicate<Throwable> toLookFor) {
        while (cause != null) {
            if (toLookFor.test(cause)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    public static <T extends Throwable> T chain(T initial, T current) {
        if (initial == null) {
            return current;
        }

        if (current != null) {
            initial.addSuppressed(current);
        }
        return initial;
    }

    public static <T extends Throwable> T withMessage(T cause, String message) {
        setMessage(cause, message);
        return cause;
    }

    private static final Field THROWABLE_MESSAGE_FIELD;

    static {
        try {
            THROWABLE_MESSAGE_FIELD = Throwable.class.getDeclaredField("detailMessage");
            THROWABLE_MESSAGE_FIELD.setAccessible(true);
        } catch (Exception e) {
            throw new LinkageError("Could not get Throwable message field", e);
        }
    }

    public static void setMessage(Throwable cause, String message) {
        try {
            THROWABLE_MESSAGE_FIELD.set(cause, message);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
