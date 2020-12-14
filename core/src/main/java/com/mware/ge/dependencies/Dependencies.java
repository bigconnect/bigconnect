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
package com.mware.ge.dependencies;

import com.mware.ge.collection.Iterables;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.impl.factory.Multimaps;

import java.util.Objects;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class Dependencies extends DependencyResolver.Adapter implements DependencySatisfier {
    private final DependencyResolver parent;
    private final MutableListMultimap<Class, Object> typeDependencies = Multimaps.mutable.list.empty();

    public Dependencies() {
        parent = null;
    }

    public Dependencies(DependencyResolver parent) {
        Objects.requireNonNull(parent);
        this.parent = parent;
    }

    @Override
    public <T> T resolveDependency(Class<T> type, SelectionStrategy selector) {
        RichIterable options = typeDependencies.get(type);
        if (options.notEmpty()) {
            return selector.select(type, (Iterable<T>) options);
        }

        // Try parent
        if (parent != null) {
            return parent.resolveDependency(type, selector);
        }

        // Out of options
        throw new UnsatisfiedDependencyException(type);
    }

    @Override
    public <T> Iterable<? extends T> resolveTypeDependencies(Class<T> type) {
        MutableList<T> options = (MutableList<T>) typeDependencies.get(type);
        if (parent != null) {
            return Iterables.concat(options, parent.resolveTypeDependencies(type));
        }
        return options;
    }

    @Override
    public <T> Supplier<T> provideDependency(final Class<T> type, final SelectionStrategy selector) {
        return () -> resolveDependency(type, selector);
    }

    @Override
    public <T> Supplier<T> provideDependency(final Class<T> type) {
        return () -> resolveDependency(type);
    }

    @Override
    public <T> T satisfyDependency(T dependency) {
        // File this object under all its possible types
        Class<?> type = dependency.getClass();
        do {
            typeDependencies.put(type, dependency);

            // Add as all interfaces
            Class<?>[] interfaces = type.getInterfaces();
            addInterfaces(interfaces, dependency);

            type = type.getSuperclass();
        }
        while (type != null);

        return dependency;
    }

    public void satisfyDependencies(Object... dependencies) {
        for (Object dependency : dependencies) {
            satisfyDependency(dependency);
        }
    }

    private <T> void addInterfaces(Class<?>[] interfaces, T dependency) {
        for (Class<?> type : interfaces) {
            typeDependencies.put(type, dependency);
            addInterfaces(type.getInterfaces(), dependency);
        }
    }
}
