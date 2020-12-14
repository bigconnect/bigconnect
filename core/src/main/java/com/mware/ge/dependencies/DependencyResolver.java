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

import java.util.Iterator;
import java.util.function.Supplier;

import static com.mware.ge.dependencies.DependencyResolver.SelectionStrategy.FIRST;

/**
 * Find a dependency given a type.
 */
public interface DependencyResolver {
    /**
     * Tries to resolve a dependency that matches a given class. No specific
     * {@link SelectionStrategy} is used, so the first encountered matching dependency will be returned.
     *
     * @param type the type of {@link Class} that the returned instance must implement.
     * @param <T>  the type that the returned instance must implement
     * @return the resolved dependency for the given type.
     * @throws IllegalArgumentException if no matching dependency was found.
     * @deprecated in next major version default selection strategy will be changed to more strict {@link DependencyResolver.SelectionStrategy#ONLY}
     */
    <T> T resolveDependency(Class<T> type) throws IllegalArgumentException;

    /**
     * Tries to resolve a dependency that matches a given class. All candidates are fed to the
     * {@code selector} which ultimately becomes responsible for making the choice between all available candidates.
     *
     * @param type     the type of {@link Class} that the returned instance must implement.
     * @param selector {@link SelectionStrategy} which will make the choice of which one to return among
     *                 matching candidates.
     * @param <T>      the type that the returned instance must implement
     * @return the resolved dependency for the given type.
     * @throws IllegalArgumentException if no matching dependency was found.
     */
    <T> T resolveDependency(Class<T> type, SelectionStrategy selector) throws IllegalArgumentException;

    /**
     * Tries to resolve a dependencies that matches a given class.
     *
     * @param type the type of {@link Class} that the returned instances must implement.
     * @param <T>  the type that the returned instance must implement
     * @return the list of resolved dependencies for the given type.
     */
    default <T> Iterable<? extends T> resolveTypeDependencies(Class<T> type) {
        throw new UnsupportedOperationException("not implemented");
    }

    <T> Supplier<T> provideDependency(Class<T> type, SelectionStrategy selector);

    <T> Supplier<T> provideDependency(Class<T> type);

    /**
     * Responsible for making the choice between available candidates.
     */
    interface SelectionStrategy {
        /**
         * Given a set of candidates, select an appropriate one. Even if there are candidates this
         * method may throw {@link IllegalArgumentException} if there was no suitable candidate.
         *
         * @param type       the type of items.
         * @param candidates candidates up for selection, where one should be picked. There might
         *                   also be no suitable candidate, in which case an exception should be thrown.
         * @param <T>        the type of items
         * @return a suitable candidate among all available.
         * @throws IllegalArgumentException if no suitable candidate was found.
         */
        <T> T select(Class<T> type, Iterable<? extends T> candidates) throws IllegalArgumentException;

        SelectionStrategy FIRST = new SelectionStrategy() {
            @Override
            public <T> T select(Class<T> type, Iterable<? extends T> candidates) throws IllegalArgumentException {
                Iterator<? extends T> iterator = candidates.iterator();
                if (!iterator.hasNext()) {
                    throw new IllegalArgumentException("Could not resolve dependency of type:" + type.getName());
                }
                return iterator.next();
            }
        };

        /**
         * Returns the one and only dependency, or throws.
         */
        SelectionStrategy ONLY = new SelectionStrategy() {
            @Override
            public <T> T select(Class<T> type, Iterable<? extends T> candidates) throws IllegalArgumentException {
                Iterator<? extends T> iterator = candidates.iterator();
                if (!iterator.hasNext()) {
                    throw new IllegalArgumentException("Could not resolve dependency of type:" + type.getName());
                }

                T only = iterator.next();

                if (iterator.hasNext()) {
                    throw new IllegalArgumentException("Multiple dependencies of type:" + type.getName());
                } else {
                    return only;
                }
            }
        };
    }

    /**
     * Adapter for {@link DependencyResolver} which will select the first available candidate by default
     * for {@link #resolveDependency(Class)}.
     *
     * @deprecated in next major version default selection strategy will be changed to more strict {@link DependencyResolver.SelectionStrategy#ONLY}
     */
    abstract class Adapter implements DependencyResolver {
        @Override
        public <T> T resolveDependency(Class<T> type) throws IllegalArgumentException {
            return resolveDependency(type, FIRST);
        }

        @Override
        public <T> Supplier<T> provideDependency(final Class<T> type, final SelectionStrategy selector) {
            return () -> resolveDependency(type, selector);
        }

        @Override
        public <T> Supplier<T> provideDependency(final Class<T> type) {
            return () -> resolveDependency(type);
        }
    }
}
