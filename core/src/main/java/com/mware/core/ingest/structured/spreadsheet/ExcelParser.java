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
package com.mware.core.ingest.structured.spreadsheet;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.structured.model.ClientApiAnalysis;
import com.mware.core.ingest.structured.model.ParseOptions;
import com.mware.core.ingest.structured.model.StructuredIngestParser;
import com.mware.core.ingest.structured.util.BaseStructuredFileParserHandler;
import com.mware.core.ingest.structured.util.StructuredFileParserHandler;
import com.mware.core.model.role.AuthorizationRepository;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.user.User;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.Graph;
import com.mware.ge.mutation.ElementMutation;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ExcelParser extends BaseParser implements StructuredIngestParser {

    private final WorkQueueRepository workQueueRepository;
    private final Graph graph;
    private final AuthorizationRepository authorizationRepository;

    @Inject
    public ExcelParser(WorkQueueRepository workQueueRepository, Graph graph, AuthorizationRepository authorizationRepository) {
        this.workQueueRepository = workQueueRepository;
        this.graph = graph;
        this.authorizationRepository = authorizationRepository;
    }

    @Override
    public Set<String> getSupportedMimeTypes() {
        return Sets.newHashSet(
                "application/xls",
                "application/excel",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
    }

    @Override
    public void ingest(InputStream in, ParseOptions parseOptions, BaseStructuredFileParserHandler parserHandler, User user) throws Exception {
        parseExcel(in, parseOptions, parserHandler, user);
    }

    @Override
    public ClientApiAnalysis analyze(InputStream inputStream, User user, Authorizations authorizations) throws Exception {
        StructuredFileParserHandler handler = new StructuredFileParserHandler(authorizations);
        handler.getHints().sendColumnIndices = true;
        handler.getHints().allowHeaderSelection = true;

        ParseOptions options = new ParseOptions();
        options.hasHeaderRow = false;
        parseExcel(inputStream, options, handler, user);
        return handler.getResult();
    }

    private void parseExcel(InputStream in, ParseOptions options, BaseStructuredFileParserHandler handler, User user) {
        try {
            Workbook workbook = WorkbookFactory.create(in);
            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
            DataFormatter formatter = new DataFormatter(true);
            List<ElementMutation<? extends Element>> batchElementBuilders = new ArrayList<>();

            int numSheets = workbook.getNumberOfSheets();
            for (int i = 0; i < numSheets; i++) {
                if (options.sheetIndex != null && i != options.sheetIndex) continue;
                Sheet excelSheet = workbook.getSheetAt(i);
                handler.newSheet(excelSheet.getSheetName());

                if (excelSheet.getPhysicalNumberOfRows() > 0) {
                    int lastRowNum = excelSheet.getLastRowNum();
                    int totalRows = 0;
                    for (int j = 0; j< lastRowNum; j++) {
                        Row row = excelSheet.getRow(j);
                        if (row != null && containsValue(row, row.getFirstCellNum(), row.getLastCellNum())) {
                            totalRows ++;
                        }
                    }
                    handler.setTotalRows(totalRows);

                    for (int j = 0, rowIndex = 0; j <= lastRowNum; j++) {
                        if (rowIndex < options.startRowIndex) {
                            rowIndex++;
                            continue;
                        }

                        Row row = excelSheet.getRow(j);
                        if(row == null)
                            continue;

                        List<Object> parsedRow = parseExcelRow(row, evaluator, formatter);
                        if (parsedRow.size() > 0 && containsValue(row, row.getFirstCellNum(), row.getLastCellNum())) {
                            if (rowIndex == options.startRowIndex && options.hasHeaderRow) {
                                for (int k = 0; k < parsedRow.size(); k++) {
                                    handler.addColumn(parsedRow.get(k).toString());
                                }
                            } else {
                                if (!handler.addRow(parsedRow, j, batchElementBuilders)) {
                                    break;
                                }

                                if (batchElementBuilders.size() > COMMIT_BATCH_SIZE) {
                                    flushData(
                                            batchElementBuilders,
                                            workQueueRepository,
                                            graph,
                                            handler.getAuthorizations()
                                    );
                                }
                            }

                            rowIndex++;
                        }
                    }

                    if (batchElementBuilders.size() > 0) {
                        flushData(
                                batchElementBuilders,
                                workQueueRepository,
                                graph,
                                handler.getAuthorizations()
                        );
                    }

                }
            }
        } catch (IOException ex) {
            throw new BcException("Could not read excel workbook", ex);
        } catch (InvalidFormatException e) {
            throw new BcException("Could not read excel workbook", e);
        }
    }

    private List<Object> parseExcelRow(Row row, FormulaEvaluator evaluator, DataFormatter formatter) {
        List<Object> parsedRow = new ArrayList<Object>();

        if (row != null) {
            int lastCellNum = row.getLastCellNum();
            for (int i = 0; i < lastCellNum; i++) {
                Cell cell = row.getCell(i);
                String cellValue = "";
                if (cell != null) {
                    if (cell.getCellType() != Cell.CELL_TYPE_FORMULA) {
                        cellValue = formatter.formatCellValue(cell);
                    } else {
                        cellValue = formatter.formatCellValue(cell, evaluator);
                    }
                }

                parsedRow.add(cellValue);
            }
        }

        return parsedRow;
    }

    public boolean containsValue(Row row, int fcell, int lcell) {
        boolean flag = false;
        for (int i = fcell; i < lcell; i++) {
            if (StringUtils.isEmpty(String.valueOf(row.getCell(i))) ||
                    StringUtils.isWhitespace(String.valueOf(row.getCell(i))) ||
                    StringUtils.isBlank(String.valueOf(row.getCell(i))) ||
                    String.valueOf(row.getCell(i)).length() == 0 ||
                    row.getCell(i) == null) {
            } else {
                flag = true;
            }
        }
        return flag;
    }

}
