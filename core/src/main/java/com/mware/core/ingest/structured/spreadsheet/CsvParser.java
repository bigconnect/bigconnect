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

import au.com.bytecode.opencsv.CSVReader;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class CsvParser extends BaseParser implements StructuredIngestParser {

    private final static String CSV_MIME_TYPE = "text/csv";

    private final WorkQueueRepository workQueueRepository;
    private final Graph graph;
    private final AuthorizationRepository authorizationRepository;

    @Inject
    public CsvParser(WorkQueueRepository workQueueRepository, Graph graph, AuthorizationRepository authorizationRepository) {
        this.workQueueRepository = workQueueRepository;
        this.graph = graph;
        this.authorizationRepository = authorizationRepository;
    }

    @Override
    public Set<String> getSupportedMimeTypes() {
        return Sets.newHashSet(CSV_MIME_TYPE);
    }

    @Override
    public void ingest(InputStream in, ParseOptions parseOptions, BaseStructuredFileParserHandler parserHandler, User user) throws Exception {
        parseCsvSheet(in, parseOptions, parserHandler, user);
    }

    @Override
    public ClientApiAnalysis analyze(InputStream inputStream, User user, Authorizations authorizations) throws Exception {
        StructuredFileParserHandler handler = new StructuredFileParserHandler(authorizations);
        handler.getHints().sendColumnIndices = true;
        handler.getHints().allowHeaderSelection = true;

        ParseOptions options = new ParseOptions();
        options.hasHeaderRow = false;
        parseCsvSheet(inputStream, options, handler, user);
        return handler.getResult();
    }

    private void parseCsvSheet(InputStream in, ParseOptions options, BaseStructuredFileParserHandler handler, User user) {
        handler.newSheet("");
        handler.setTotalRows(getTotalRows(in, options));

        List<ElementMutation<? extends Element>> batchElementBuilders = new ArrayList<>();

        try (Reader reader = new InputStreamReader(in)) {
            int row = 0;
            try (CSVReader csvReader = new CSVReader(reader, options.separator, options.quoteChar)) {
                String[] columnValues;

                while ((columnValues = csvReader.readNext()) != null) {
                    if (row < options.startRowIndex) {
                        row++;
                        continue;
                    }
                    if (rowIsBlank(columnValues)) {
                        continue;
                    }

                    if (row == options.startRowIndex && options.hasHeaderRow) {
                        for (String headerColumn : columnValues) {
                            handler.addColumn(headerColumn);
                        }
                    } else {
                        if (!handler.addRow(Arrays.asList(columnValues), row, batchElementBuilders)) {
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
                    row++;
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
        } catch (IOException ex) {
            throw new BcException("Could not read csv", ex);
        }
    }
}

