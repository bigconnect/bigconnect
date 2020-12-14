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
package com.mware.core.lifecycle;

/**
 * This exception is thrown by LifeSupport if a lifecycle transition fails. If many exceptions occur
 * they will be chained through the cause exception mechanism.
 */
public class LifecycleException
        extends RuntimeException {

    public LifecycleException(Object instance, LifecycleStatus from, LifecycleStatus to, Throwable cause) {
        super(humanReadableMessage(instance, from, to, cause), cause);
    }

    public LifecycleException(String message, Throwable cause) {
        super(message, cause);
    }

    private static String humanReadableMessage(Object instance, LifecycleStatus from,
                                               LifecycleStatus to, Throwable cause) {
        String instanceStr = String.valueOf(instance);
        StringBuilder message = new StringBuilder();
        switch (to) {
            case STOPPED:
                if (from == LifecycleStatus.NONE) {
                    message.append("Component '").append(instanceStr).append("' failed to initialize");
                } else if (from == LifecycleStatus.STARTED) {
                    message.append("Component '").append(instanceStr).append("' failed to stop");
                }
                break;
            case STARTED:
                if (from == LifecycleStatus.STOPPED) {
                    message.append("Component '").append(instanceStr)
                            .append("' was successfully initialized, but failed to start");
                }
                break;
            case SHUTDOWN:
                message.append("Component '").append(instanceStr).append("' failed to shut down");
                break;
            default:
                break;
        }
        if (message.length() == 0) {
            message.append("Component '").append(instanceStr).append("' failed to transition from ")
                    .append(from.name().toLowerCase()).append(" to ").append(to.name().toLowerCase());
        }
        message.append('.');
        if (cause != null) {
            Throwable root = rootCause(cause);
            message.append(" Please see the attached cause exception \"").append(root.getMessage()).append('"');
            if (root.getCause() != null) {
                message.append(" (root cause cycle detected)");
            }
            message.append('.');
        }

        return message.toString();
    }

    private static Throwable rootCause(Throwable cause) {
        int i = 0; // Guard against infinite self-cause exception-loops.
        while (cause.getCause() != null && i++ < 100) {
            cause = cause.getCause();
        }
        return cause;
    }
}
