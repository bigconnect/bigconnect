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
package com.mware.ge.values.storable.random;

/**
 * This class is meant as a gap so that we don't need a direct dependency on a random number generator.
 * <p>
 * For example by wrapping in a generator we can support both {@code java.util.Random} and {@code java.util
 * .SplittableRandom}
 */
public interface Generator {
    /**
     * Return a pseudorandom normally distributed long
     *
     * @return a pseudorandom normally distributed long
     */
    long nextLong();

    /**
     * Return a pseudorandom normally distributed boolean
     *
     * @return a pseudorandom normally distributed boolean
     */
    boolean nextBoolean();

    /**
     * Return a pseudorandom normally distributed int
     *
     * @return a pseudorandom normally distributed int
     */
    int nextInt();

    /**
     * Return a pseudorandom normally distributed long between 0 (inclusive) and the given bound(exlusive)
     *
     * @param bound the exclusive upper bound for the number generation
     * @return a pseudorandom normally distributed int
     */
    int nextInt(int bound);

    /**
     * Return a pseudorandom normally distributed float from {@code 0.0f} (inclusive) to {@code 1.0f} (exclusive)
     *
     * @return a pseudorandom normally distributed from {@code 0.0f} (inclusive) to {@code 1.0f} (exclusive)
     */
    float nextFloat();

    /**
     * Return a pseudorandom normally distributed double from {@code 0.0} (inclusive) to {@code 1.0} (exclusive)
     *
     * @return a pseudorandom normally distributed double from {@code 0.0} (inclusive) to {@code 1.0} (exclusive)
     */
    double nextDouble();
}
