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
import com.mware.core.exception.BcException;
import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.ingest.structured.model.ParseOptions;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.ge.Authorizations;
import com.mware.ge.Element;
import com.mware.ge.Graph;
import com.mware.ge.mutation.ElementMutation;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

public abstract class BaseParser {
    protected static int COMMIT_BATCH_SIZE = 400;

    protected boolean rowIsBlank(String[] columnValues) {
        // skip over blank rows
        boolean allBlank = true;
        for (int i = 0; i < columnValues.length && allBlank; i++) {
            allBlank = allBlank && StringUtils.isBlank(columnValues[i]);
        }
        return allBlank;
    }

    protected int getTotalRows(InputStream in, ParseOptions options) {
        try (Reader reader = new InputStreamReader(in)) {
            int row = 0;
            try (CSVReader csvReader = new CSVReader(reader, options.separator, options.quoteChar)) {
                String[] columnValues;
                while ((columnValues = csvReader.readNext()) != null) {
                    if (rowIsBlank(columnValues)) {
                        continue;
                    }
                    row++;
                }
                in.reset();
                return row;
            }
        } catch (IOException e) {
            throw new BcException("Could not read csv", e);
        }
    }

    protected void flushData(List<ElementMutation<? extends Element>> batchElementBuilders, WorkQueueRepository workQueueRepository, Graph graph, Authorizations authorizations) {
        Iterable<Element> elements = graph.saveElementMutations(batchElementBuilders, authorizations);

        workQueueRepository.pushMultipleElementOnDwQueue(
                elements,
                null,
                null,
                null,
                null,
                Priority.LOW,
                ElementOrPropertyStatus.UPDATE,
                null
        );
        graph.flush();
        batchElementBuilders.clear();
    }
}
