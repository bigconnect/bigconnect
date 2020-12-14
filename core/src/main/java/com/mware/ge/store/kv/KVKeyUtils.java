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
package com.mware.ge.store.kv;

import com.mware.ge.store.StoreKey;
import com.mware.ge.store.mutations.StoreMutation;

import java.nio.ByteBuffer;

public class KVKeyUtils {
    public static final char ID_VALUE_SEPARATOR = '\u001e';

    public static byte[] encodeId(byte[] id) {
        byte[] _id = new byte[id.length + 1];
        System.arraycopy(id, 0, _id, 0, id.length);
        _id[id.length] = ID_VALUE_SEPARATOR;
        return _id;
    }

    public static byte[] decodeId(byte[] id) {
        int idlen = 0;
        for (int i = 0; i < id.length; i++) {
            if (id[i] == ID_VALUE_SEPARATOR) {
                idlen = i;
                break;
            }
        }

        byte[] bid = new byte[idlen];
        System.arraycopy(id, 0, bid, 0, idlen);
        return bid;
    }

    public static ByteBuffer keyFromMutation(StoreMutation m, byte[] cf, byte[] cq, byte[] vis) {
        byte[] id = encodeId(m.getRow());
        ByteBuffer buf = ByteBuffer.allocate(
                id.length // id
                        + Integer.BYTES // cf.length
                        + cf.length
                        + Integer.BYTES // cq.length
                        + cq.length
                        + Integer.BYTES // vis.length
                        + vis.length
        );

        buf.put(id)
                .putInt(cf.length)
                .put(cf)
                .putInt(cq.length)
                .put(cq)
                .putInt(vis.length)
                .put(vis);

        return buf;
    }

    public static StoreKey storeKey(byte[] key) {
        byte[] bid = decodeId(key);
        int idlen = bid.length;

        ByteBuffer buffer = ByteBuffer.wrap(key, idlen + 1, key.length - idlen - 1);
        byte[] id = bid;
        byte[] bcf = new byte[buffer.getInt()];
        buffer.get(bcf);
        byte[] bcq = new byte[buffer.getInt()];
        buffer.get(bcq);
        byte[] vis = new byte[buffer.getInt()];
        buffer.get(vis);
        return new StoreKey(id, bcf, bcq, vis);
    }
}
