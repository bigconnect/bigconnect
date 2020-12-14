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
package com.mware.ge.values.storable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.unmodifiableSet;

public class TimeZones {
    /**
     * Prevent instance creation.
     */
    private TimeZones() {
    }

    private static final List<String> TIME_ZONE_SHORT_TO_STRING = new ArrayList<>(1024);
    private static final Map<String, Short> TIME_ZONE_STRING_TO_SHORT = new HashMap<>(1024);

    private static final long MIN_ZONE_OFFSET_SECONDS = -18 * 3600;
    private static final long MAX_ZONE_OFFSET_SECONDS = 18 * 3600;

    public static boolean validZoneOffset(int zoneOffsetSeconds) {
        return zoneOffsetSeconds >= MIN_ZONE_OFFSET_SECONDS && zoneOffsetSeconds <= MAX_ZONE_OFFSET_SECONDS;
    }

    public static boolean validZoneId(short zoneId) {
        return zoneId >= 0 && zoneId < TIME_ZONE_SHORT_TO_STRING.size();
    }

    public static final String LATEST_SUPPORTED_IANA_VERSION;

    /**
     * @throws IllegalArgumentException if tzid is not in the file
     */
    public static short map(String tzid) {
        if (!TIME_ZONE_STRING_TO_SHORT.containsKey(tzid)) {
            throw new IllegalArgumentException("tzid");
        }
        return TIME_ZONE_STRING_TO_SHORT.get(tzid);
    }

    public static String map(short offset) {
        return TIME_ZONE_SHORT_TO_STRING.get(offset);
    }

    public static Set<String> supportedTimeZones() {
        return unmodifiableSet(TIME_ZONE_STRING_TO_SHORT.keySet());
    }

    static {
        String latestVersion = "";
        Pattern version = Pattern.compile("# tzdata([0-9]{4}[a-z])");
        Map<String, String> oldToNewName = new HashMap<>(1024);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TimeZones.class.getResourceAsStream("/TZIDS")))) {
            for (String line; (line = reader.readLine()) != null; ) {
                if (line.startsWith("//") || line.trim().isEmpty()) {
                    continue;
                } else if (line.startsWith("#")) {
                    Matcher matcher = version.matcher(line);
                    if (matcher.matches()) {
                        latestVersion = matcher.group(1);
                    }
                    continue;
                }
                int sep = line.indexOf(' ');
                if (sep != -1) {
                    String oldName = line.substring(0, sep);
                    String newName = line.substring(sep + 1);
                    TIME_ZONE_SHORT_TO_STRING.add(newName);
                    oldToNewName.put(oldName, newName);
                } else {
                    TIME_ZONE_SHORT_TO_STRING.add(line);
                    TIME_ZONE_STRING_TO_SHORT.put(line, (short) (TIME_ZONE_SHORT_TO_STRING.size() - 1));
                }
            }
            LATEST_SUPPORTED_IANA_VERSION = latestVersion;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read time zone id file.");
        }

        for (Map.Entry<String, String> entry : oldToNewName.entrySet()) {
            String oldName = entry.getKey();
            String newName = entry.getValue();
            Short newNameId = TIME_ZONE_STRING_TO_SHORT.get(newName);
            TIME_ZONE_STRING_TO_SHORT.put(oldName, newNameId);
        }
    }
}
