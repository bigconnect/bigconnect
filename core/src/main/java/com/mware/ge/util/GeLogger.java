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
package com.mware.ge.util;

import org.slf4j.Logger;

public class GeLogger {
    private final Logger logger;

    public GeLogger(final Logger logger) {
        this.logger = logger;
    }

    public void trace(final String format, final Object... args) {
        if (isDebugEnabled()) {
            logger.trace(format(format, args), findLastThrowable(args));
        }
    }

    public void trace(final String message, final Throwable t) {
        if (isDebugEnabled()) {
            logger.trace(message, t);
        }
    }

    public void debug(final String format, final Object... args) {
        if (isDebugEnabled()) {
            logger.debug(format(format, args), findLastThrowable(args));
        }
    }

    public void debug(final String message, final Throwable t) {
        if (isDebugEnabled()) {
            logger.debug(message, t);
        }
    }

    public void info(final String format, final Object... args) {
        if (isInfoEnabled()) {
            logger.info(format(format, args), findLastThrowable(args));
        }
    }

    public void info(final String message, final Throwable t) {
        if (isInfoEnabled()) {
            logger.info(message, t);
        }
    }

    public void warn(final String format, final Object... args) {
        if (logger.isWarnEnabled()) {
            logger.warn(format(format, args), findLastThrowable(args));
        }
    }

    public void warn(final String message, final Throwable t) {
        if (logger.isWarnEnabled()) {
            logger.warn(message, t);
        }
    }

    public void error(final String format, final Object... args) {
        if (logger.isErrorEnabled()) {
            logger.error(format(format, args), findLastThrowable(args));
        }
    }

    public void error(final String message, final Throwable t) {
        if (logger.isErrorEnabled()) {
            logger.error(message, t);
        }
    }

    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    private String format(final String format, final Object[] args) {
        try {
            if (args.length == 0) {
                return format;
            }
            return String.format(format, args);
        } catch (Exception ex) {
            error("Invalid format string: " + format, ex);
            StringBuilder sb = new StringBuilder();
            sb.append(format);
            for (Object arg : args) {
                sb.append(", ");
                sb.append(arg);
            }
            return sb.toString();
        }
    }

    private Throwable findLastThrowable(final Object[] args) {
        int length = args != null ? args.length : 0;
        return (length > 0 && args[length - 1] instanceof Throwable) ? (Throwable) args[length - 1] : null;
    }

    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }
}
