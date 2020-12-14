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
package com.mware.core.bootstrap;

import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.ProvisionException;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ServiceLoaderUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mware.ge.util.IterableUtils.toList;

public class InjectHelper {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(InjectHelper.class);
    private static Injector injector;

    public static <T> T inject(T o, ModuleMaker moduleMaker, Configuration configuration) {
        ensureInjectorCreated(moduleMaker, configuration);
        inject(o);
        return o;
    }

    public static <T> T inject(T o) {
        if (injector == null) {
            throw new BcException("Could not find injector");
        }
        injector.injectMembers(o);
        return o;
    }

    public static Injector getInjector() {
        return injector;
    }

    public static <T> T getInstance(Class<T> clazz, ModuleMaker moduleMaker, Configuration configuration) {
        ensureInjectorCreated(moduleMaker, configuration);
        return injector.getInstance(clazz);
    }

    public static <T> T getInstance(Class<? extends T> clazz) {
        LOGGER.debug("getInstance of class: " + clazz.getName());
        if (injector == null) {
            throw new BcException("Could not find injector");
        }
        try {
            return injector.getInstance(clazz);
        } catch (ProvisionException ex) {
            LOGGER.error("Could not create class: %s: %s", clazz.getName(), ex.getMessage(), ex);
            throw new BcException("Could not create class: " + clazz.getName(), ex);
        } catch (Throwable ex) {
            throw new BcException("Could not create class: " + clazz.getName(), ex);
        }
    }

    public static <T> Collection<T> getInjectedServices(Class<T> clazz, Configuration configuration) {
        List<Class<? extends T>> serviceClasses = toList(ServiceLoaderUtil.loadClasses(clazz, configuration));
        Collection<T> results = new ArrayList<>();
        for (Class<? extends T> serviceClass : serviceClasses) {
            results.add(getInstance(serviceClass));
        }
        return results;
    }

    public static void shutdown() {
        injector = null;
    }

    public static boolean hasInjector() {
        return injector != null;
    }

    @VisibleForTesting
    public static void setInjector(Injector injector) {
        InjectHelper.injector = injector;
    }

    public interface ModuleMaker {
        Module createModule();

        Configuration getConfiguration();
    }

    private static void ensureInjectorCreated(ModuleMaker moduleMaker, Configuration configuration) {
        if (injector == null) {
            injector = Guice.createInjector(moduleMaker.createModule(), new ObjectMapperModule());
        }
    }
}
