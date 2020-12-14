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
package com.mware.core.util;

import com.mware.core.exception.BcException;

import java.io.UnsupportedEncodingException;

public class LenientURLDecoder {
    public static String decode(String s, String enc) {
        try {
            boolean needToChange = false;
            StringBuilder sb = new StringBuilder();
            int numChars = s.length();
            int i = 0;

            if (enc.length() == 0) {
                throw new UnsupportedEncodingException("URLDecoder: empty string enc parameter");
            }

            while (i < numChars) {
                char c = s.charAt(i);
                switch (c) {
                    case '+':
                        sb.append(' ');
                        i++;
                        needToChange = true;
                        break;
                    case '%':
                   /*
                    * Starting with this instance of %, process all
                    * consecutive substrings of the form %xy. Each
                    * substring %xy will yield a byte. Convert all
                    * consecutive bytes obtained this way to whatever
                    * character(s) they represent in the provided
                    * encoding.
                    */

                        // (numChars-i)/3 is an upper bound for the number
                        // of remaining bytes
                        byte[] bytes = new byte[(numChars - i) / 3];
                        int pos = 0;

                        while (((i + 2) < numChars) &&
                                (c == '%')) {
                            String hex = s.substring(i + 1, i + 3);
                            try {
                                bytes[pos] =
                                        (byte) Integer.parseInt(hex, 16);
                                pos++;
                            } catch (NumberFormatException e) {
                                sb.append(new String(bytes, 0, pos, enc));
                                sb.append("%");
                                sb.append(hex);
                                pos = 0;
                            }
                            i += 3;
                            if (i < numChars)
                                c = s.charAt(i);
                        }

                        sb.append(new String(bytes, 0, pos, enc));

                        // A trailing, incomplete byte encoding such as
                        // "%x" will be treated as unencoded text
                        if ((i < numChars) && (c == '%')) {
                            for (; i < numChars; i++) {
                                sb.append(s.charAt(i));
                            }
                        }

                        needToChange = true;
                        break;
                    default:
                        sb.append(c);
                        i++;
                        break;
                }
            }

            return (needToChange ? sb.toString() : s);
        } catch (Exception ex) {
            throw new BcException("Could not url decode string \"" + s + "\"", ex);
        }
    }
}
