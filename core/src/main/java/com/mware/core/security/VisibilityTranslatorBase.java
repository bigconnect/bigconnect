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
package com.mware.core.security;

import com.mware.ge.Visibility;
import com.mware.core.model.clientapi.dto.VisibilityJson;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class VisibilityTranslatorBase extends VisibilityTranslator {
    @Override
    public BcVisibility toVisibility(VisibilityJson visibilityJson) {
        return new BcVisibility(toVisibilityNoSuperUser(visibilityJson));
    }

    @Override
    public BcVisibility toVisibility(String visibilitySource) {
        return toVisibility(visibilitySourceToVisibilityJson(visibilitySource));
    }

    protected VisibilityJson visibilitySourceToVisibilityJson(String visibilitySource) {
        return new VisibilityJson(visibilitySource);
    }

    @Override
    public Visibility toVisibilityNoSuperUser(VisibilityJson visibilityJson) {
        StringBuilder visibilityString = new StringBuilder();

        List<String> required = new ArrayList<>();

        String source = visibilityJson.getSource();
        addSourceToRequiredVisibilities(required, source);

        Set<String> workspaces = visibilityJson.getWorkspaces();
        if (workspaces != null) {
            required.addAll(workspaces);
        }

        for (String v : required) {
            if (visibilityString.length() > 0) {
                visibilityString.append("&");
            }
            visibilityString
                    .append("(")
                    .append(v)
                    .append(")");
        }
        return new Visibility(visibilityString.toString());
    }

    protected abstract void addSourceToRequiredVisibilities(List<String> required, String source);

    @Override
    public Visibility getDefaultVisibility() {
        return new Visibility("");
    }
}
