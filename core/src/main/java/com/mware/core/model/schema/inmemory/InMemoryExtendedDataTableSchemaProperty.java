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
package com.mware.core.model.schema.inmemory;

import com.google.common.collect.ImmutableList;
import com.mware.core.model.schema.ExtendedDataTableProperty;
import com.mware.core.model.properties.SchemaProperties;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;

import java.util.ArrayList;
import java.util.List;

public class InMemoryExtendedDataTableSchemaProperty extends InMemorySchemaProperty implements ExtendedDataTableProperty {
    private String titleFormula;
    private String subtitleFormula;
    private String timeFormula;
    private List<String> tablePropertyNames = new ArrayList<>();

    @Override
    public String getTitleFormula() {
        return titleFormula;
    }

    public void setTitleFormula(String titleFormula) {
        this.titleFormula = titleFormula;
    }

    @Override
    public String getSubtitleFormula() {
        return subtitleFormula;
    }

    public void setSubtitleFormula(String subtitleFormula) {
        this.subtitleFormula = subtitleFormula;
    }

    @Override
    public String getTimeFormula() {
        return timeFormula;
    }

    @Override
    public ImmutableList<String> getTablePropertyNames() {
        return ImmutableList.copyOf(tablePropertyNames);
    }

    public void addTableProperty(String tablePropertyName) {
        tablePropertyNames.add(tablePropertyName);
    }

    public void setTimeFormula(String timeFormula) {
        this.timeFormula = timeFormula;
    }

    @Override
    public void setProperty(String name, Value value, User user, Authorizations authorizations) {
        if (SchemaProperties.TITLE_FORMULA.getPropertyName().equals(name)) {
            this.titleFormula = ((TextValue) value).stringValue();
        } else if (SchemaProperties.SUBTITLE_FORMULA.getPropertyName().equals(name)) {
            this.subtitleFormula = ((TextValue) value).stringValue();
        } else if (SchemaProperties.TIME_FORMULA.getPropertyName().equals(name)) {
            this.timeFormula = ((TextValue) value).stringValue();
        } else {
            super.setProperty(name, value, user, authorizations);
        }
    }
}
