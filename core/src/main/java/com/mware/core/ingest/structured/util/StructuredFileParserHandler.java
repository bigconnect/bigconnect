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
 *
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
package com.mware.core.ingest.structured.util;

import com.mware.core.exception.BcException;
import com.mware.core.ingest.structured.mapping.ColumnMappingType;
import com.mware.core.ingest.structured.model.ClientApiAnalysis;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.mutation.ElementMutation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StructuredFileParserHandler extends BaseStructuredFileParserHandler {
    private ClientApiAnalysis result = new ClientApiAnalysis();
    private ClientApiAnalysis.Sheet currentSheet;

    public StructuredFileParserHandler(Authorizations authorizations) {
        super(authorizations);
    }

    public ClientApiAnalysis getResult() {
        return this.result;
    }

    @Override
    public void newSheet(String name) {
        currentSheet = new ClientApiAnalysis.Sheet();
        currentSheet.name = name;
        result.sheets.add(currentSheet);
    }

    public ClientApiAnalysis.Hints getHints() {
        return result.hints;
    }

    @Override
    public void addColumn(String name, ColumnMappingType type) {
        ClientApiAnalysis.Column column = new ClientApiAnalysis.Column();
        column.name = name;
        column.type = type;
        currentSheet.columns.add(column);
    }

    @Override
    public boolean addRow(List<Object> values, long rowNum, List<ElementMutation<? extends Element>> batchElementBuilders) {
        ClientApiAnalysis.ParsedRow parsedRow = new ClientApiAnalysis.ParsedRow();
        parsedRow.columns.addAll(values);
        currentSheet.parsedRows.add(parsedRow);
        return currentSheet.parsedRows.size() < 10;
    }

    @Override
    public void setTotalRows(long rows) {
        super.setTotalRows(rows);
        currentSheet.totalRows = rows;
    }

    @Override
    public boolean addRow(Map<String, Object> row, long rowNum, List<ElementMutation<? extends Element>> batchElementBuilders) {
        if (currentSheet.columns.size() == 0) {
            throw new BcException("Set columns before rows");
        }
        List<Object> values = new ArrayList<>(row.size());
        for (ClientApiAnalysis.Column column : currentSheet.columns) {
            Object value = row.get(column.name);
            values.add(value == null ? "" : value);
        }

        return addRow(values, rowNum, batchElementBuilders);
    }
}
