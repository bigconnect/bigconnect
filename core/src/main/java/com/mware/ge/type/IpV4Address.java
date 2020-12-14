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
package com.mware.ge.type;

import com.mware.ge.GeException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpV4Address implements Serializable, Comparable<IpV4Address> {
    private static final long serialVersionUID = 42L;
    private static final Pattern IP_REGEX = Pattern.compile("^([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})\\.([0-9]{1,3})$");
    private final int[] octets;

    public IpV4Address(String ipAddress) {
        Matcher m = IP_REGEX.matcher(ipAddress);
        if (!m.matches()) {
            throw new GeException("Could not parse IP address: " + ipAddress);
        }
        octets = new int[4];
        for (int i = 0; i < 4; i++) {
            octets[i] = Integer.parseInt(m.group(i + 1));
        }
    }

    public IpV4Address(int a, int b, int c, int d) {
        this(new int[]{a, b, c, d});
    }

    public IpV4Address(int[] octets) {
        if (octets.length != 4) {
            throw new GeException("Invalid IP address. Expected 4 octets, found " + octets.length);
        }
        this.octets = Arrays.copyOf(octets, 4);
    }

    public IpV4Address(byte[] octets) {
        if (octets.length != 4) {
            throw new GeException("Invalid IP address. Expected 4 octets, found " + octets.length);
        }
        this.octets = new int[4];
        for (int i = 0; i < 4; i++) {
            this.octets[i] = octets[i];
        }
    }

    @Override
    public String toString() {
        return octets[0] + "." + octets[1] + "." + octets[2] + "." + octets[3];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IpV4Address ipAddress = (IpV4Address) o;

        if (!Arrays.equals(octets, ipAddress.octets)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return octets != null ? Arrays.hashCode(octets) : 0;
    }

    @Override
    public int compareTo(IpV4Address o) {
        for (int i = 0; i < 4; i++) {
            int eq = Integer.compare(this.octets[i], o.octets[i]);
            if (eq != 0) {
                return eq;
            }
        }
        return 0;
    }
}
