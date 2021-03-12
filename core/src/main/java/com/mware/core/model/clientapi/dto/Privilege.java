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
package com.mware.core.model.clientapi.dto;

import com.mware.core.model.clientapi.util.StringUtils;
import org.json.JSONArray;

import java.util.*;

public final class Privilege {
    public static final String READ = "READ";
    public static final String COMMENT = "COMMENT"; // add comments and edit/delete own comments
    public static final String COMMENT_EDIT_ANY = "COMMENT_EDIT_ANY"; // edit other users' comments
    public static final String COMMENT_DELETE_ANY = "COMMENT_DELETE_ANY"; // delete other users' comments
    public static final String HISTORY_READ = "HISTORY_READ"; // read vertex/edge/property history
    public static final String SEARCH_SAVE_GLOBAL = "SEARCH_SAVE_GLOBAL";
    public static final String EDIT = "EDIT";
    public static final String UNRESOLVE = "UNRESOLVE";
    public static final String PUBLISH = "PUBLISH";
    public static final String ADMIN = "ADMIN";
    public static final String ONTOLOGY_ADD = "ONTOLOGY_ADD"; // Add to ontology from interface
    public static final String ONTOLOGY_PUBLISH = "ONTOLOGY_PUBLISH"; // Add to ontology from interface

    static {
        // NOTE: Keep allNames in sync with the above public static strings.
        final String[] allNames = new String[] {
                READ,
                COMMENT,
                COMMENT_EDIT_ANY,
                COMMENT_DELETE_ANY,
                HISTORY_READ,
                SEARCH_SAVE_GLOBAL,
                EDIT,
                PUBLISH,
                ADMIN,
                ONTOLOGY_ADD,
                ONTOLOGY_PUBLISH,
                UNRESOLVE
        };
        Set<Privilege> allPrivileges = new HashSet<Privilege>(allNames.length);
        for (String name : allNames) {
            allPrivileges.add(new Privilege(name));
        }
        ALL_BUILT_IN = Collections.unmodifiableSet(allPrivileges);
    }

    public static final Set<Privilege> ALL_BUILT_IN;

    private final String name;

    public Privilege(String name) {
        this.name = name;
    }

    public static Set<String> newSet(String... privileges) {
        Set<String> set = new HashSet<String>();
        Collections.addAll(set, privileges);
        return Collections.unmodifiableSet(set);
    }

    private static List<String> sortPrivileges(Iterable<String> privileges) {
        List<String> sortedPrivileges = new ArrayList<String>();
        for (String privilege : privileges) {
            sortedPrivileges.add(privilege);
        }
        Collections.sort(sortedPrivileges);
        return sortedPrivileges;
    }

    public static JSONArray toJson(Set<String> privileges) {
        JSONArray json = new JSONArray();
        for (String privilege : sortPrivileges(privileges)) {
            json.put(privilege);
        }
        return json;
    }

    public static Set<String> stringToPrivileges(String privilegesString) {
        if (privilegesString == null || privilegesString.equalsIgnoreCase("NONE")) {
            return Collections.emptySet();
        }

        String[] privilegesStringParts = privilegesString.split(",");
        Set<String> privileges = new HashSet<String>();
        for (String privilegesStringPart : privilegesStringParts) {
            if (privilegesStringPart.trim().length() == 0) {
                continue;
            }
            privileges.add(privilegesStringPart.trim());
        }
        return privileges;
    }

    public static String toString(Iterable<String> privileges) {
        return StringUtils.join(sortPrivileges(privileges));
    }

    public static String toStringPrivileges(Iterable<Privilege> privileges) {
        Collection<String> privilegeStrings = new ArrayList<String>();
        for (Privilege privilege : privileges) {
            privilegeStrings.add(privilege.getName());
        }
        return toString(privilegeStrings);
    }

    public static boolean hasAll(Set<String> userPrivileges, Set<String> requiredPrivileges) {
        for (String privilege : requiredPrivileges) {
            if (!userPrivileges.contains(privilege)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return getName();
    }

    public String getName() {
        return name;
    }
}
