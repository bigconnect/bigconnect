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
package com.mware.core.ingest.database;

import com.google.inject.Inject;
import com.mware.core.exception.BcException;
import com.mware.core.ingest.database.model.ClientApiDataSource;
import com.mware.core.ingest.structured.util.ProgressReporter;
import com.mware.core.model.Description;
import com.mware.core.model.Name;
import com.mware.core.model.longRunningProcess.LongRunningProcessRepository;
import com.mware.core.model.longRunningProcess.LongRunningProcessWorker;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.core.util.ClientApiConverter;
import org.json.JSONObject;

import java.text.NumberFormat;
import java.time.ZonedDateTime;

@Name("Data Source Import")
@Description("Loads data from a data source into the graph")
public class DataSourceImportLongRunningProcess extends LongRunningProcessWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(DataSourceImportLongRunningProcess.class);

    public static final String TYPE = "datasource-ingest";

    private LongRunningProcessRepository longRunningProcessRepository;
    private DataConnectionRepository dataConnectionRepository;

    @Override
    public boolean isHandled(JSONObject longRunningProcessQueueItem) {
        //Right now, processes are restarted after a server crash. We can add condition here to avoid this, if the process is not in the JobRegistry
        return TYPE.equals(longRunningProcessQueueItem.getString("type"));
    }

    @Override
    protected void processInternal(final JSONObject longRunningProcessQueueItem) {
        ClientApiDataSource queueItem =
                ClientApiConverter.toClientApi(longRunningProcessQueueItem.toString(), ClientApiDataSource.class);
        if (queueItem.isCanceled()) {
            LOGGER.info("DataSource import long running process was canceled. Nothing to do");
            return;
        }
        longRunningProcessRepository.reportProgress(longRunningProcessQueueItem, 0, "Starting import");

        NumberFormat numberFormat = NumberFormat.getIntegerInstance();

        ProgressReporter reporter = new ProgressReporter() {
            public void finishedRow(long row, long totalRows) {
                if (totalRows != -1) {
                    longRunningProcessRepository.reportProgress(
                            longRunningProcessQueueItem,
                            ((float)row) / ((float) totalRows),
                            "Row " + numberFormat.format(row) + " of " + numberFormat.format(totalRows));
                }
            }
        };

        try {
            //This must be here for when server restarts and requeues the jobs
            dataConnectionRepository.setImportRunning(queueItem.getDsId(), true);

            dataConnectionRepository.getDataSourceManager().startImport(queueItem, reporter);
        } catch (Exception e) {
            stopDSImport(queueItem);
            e.printStackTrace();
            throw new BcException("Unable to ingest" , e);
        } finally {
           stopDSImport(queueItem);
        }
    }

    private void stopDSImport(ClientApiDataSource queueItem) {
        dataConnectionRepository.setImportRunning(queueItem.getDsId(), false);
        dataConnectionRepository.setLastImportDate(queueItem.getDsId(), ZonedDateTime.now());
    }

    @Inject
    public void setLongRunningProcessRepository(LongRunningProcessRepository longRunningProcessRepository) {
        this.longRunningProcessRepository = longRunningProcessRepository;
    }

    @Inject
    public void setDataConnectionRepository(DataConnectionRepository dataConnectionRepository) {
        this.dataConnectionRepository = dataConnectionRepository;
    }
}
