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
package com.mware.ge.accumulo;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import com.mware.ge.util.GeLogger;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.SortedSet;

public class AccumuloGraphLogger {
    private final GeLogger queryLogger;

    public AccumuloGraphLogger(GeLogger queryLogger) {
        this.queryLogger = queryLogger;
    }

    public void logStartIterator(String table, ScannerBase scanner) {
        if (!queryLogger.isTraceEnabled()) {
            return;
        }

        SortedSet<Column> fetchedColumns = null;
        if (scanner instanceof ScannerOptions) {
            fetchedColumns = ((ScannerOptions) scanner).getFetchedColumns();
        }

        if (scanner instanceof BatchScanner) {
            try {
                Field rangesField = scanner.getClass().getDeclaredField("ranges");
                rangesField.setAccessible(true);
                ArrayList<Range> ranges = (ArrayList<Range>) rangesField.get(scanner);
                if (ranges.size() == 0) {
                    logStartIterator(table, (Range) null, fetchedColumns);
                } else if (ranges.size() == 1) {
                    logStartIterator(table, ranges.iterator().next(), fetchedColumns);
                } else {
                    logStartIterator(table, ranges, fetchedColumns);
                }
            } catch (Exception e) {
                queryLogger.trace("Could not get ranges from BatchScanner", e);
            }
        } else if (scanner instanceof Scanner) {
            Range range = ((Scanner) scanner).getRange();
            logStartIterator(table, range, fetchedColumns);
        } else {
            queryLogger.trace("begin accumulo iterator: %s", scanner.getClass().getName());
        }
    }

    private void logStartIterator(String table, Range range, SortedSet<Column> fetchedColumns) {
        String fetchedColumnsString = fetchedColumnsToString(fetchedColumns);
        if (range == null || (range.getStartKey() == null && range.getEndKey() == null)) {
            queryLogger.trace("begin accumulo iterator %s: (%s): all items", table, fetchedColumnsString);
        } else {
            queryLogger.trace("begin accumulo iterator %s: (%s): %s - %s", table, fetchedColumnsString, keyToString(range.getStartKey()), keyToString(range.getEndKey()));
        }
    }

    private void logStartIterator(String table, ArrayList<Range> ranges, SortedSet<Column> fetchedColumns) {
        String fetchedColumnsString = fetchedColumnsToString(fetchedColumns);
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Range r : ranges) {
            if (!first) {
                sb.append("\n");
            }
            sb.append("  ").append(keyToString(r.getStartKey())).append(" - ").append(keyToString(r.getEndKey()));
            first = false;
        }
        queryLogger.trace("begin accumulo iterator %s: (%s):\n%s", table, fetchedColumnsString, sb.toString());
    }

    private String keyToString(Key key) {
        if (key == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        appendText(sb, key.getRow());
        if (key.getColumnFamily() != null && key.getColumnFamily().getLength() > 0) {
            sb.append(":");
            appendText(sb, key.getColumnFamily());
        }
        if (key.getColumnQualifier() != null && key.getColumnQualifier().getLength() > 0) {
            sb.append(":");
            appendText(sb, key.getColumnQualifier());
        }
        if (key.getColumnVisibility() != null && key.getColumnVisibility().getLength() > 0) {
            sb.append(":");
            appendText(sb, key.getColumnVisibility());
        }
        if (key.getTimestamp() != Long.MAX_VALUE) {
            sb.append(":");
            sb.append(key.getTimestamp());
        }
        return sb.toString();
    }

    private String fetchedColumnsToString(SortedSet<Column> fetchedColumns) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Column fetchedColumn : fetchedColumns) {
            if (!first) {
                sb.append(",");
            }
            sb.append(fetchedColumnToString(fetchedColumn));
            first = false;
        }
        return sb.toString();
    }

    private String fetchedColumnToString(Column fetchedColumn) {
        StringBuilder sb = new StringBuilder();
        appendBytes(sb, fetchedColumn.getColumnFamily());
        if (fetchedColumn.getColumnQualifier() != null) {
            sb.append(":");
            appendBytes(sb, fetchedColumn.getColumnQualifier());
        }
        if (fetchedColumn.getColumnVisibility() != null) {
            sb.append(":");
            appendBytes(sb, fetchedColumn.getColumnVisibility());
        }
        return sb.toString();
    }

    private void appendText(StringBuilder sb, Text text) {
        String str = text.toString();
        for (char c : str.toCharArray()) {
            if (c >= ' ' && c <= '~') {
                sb.append(c);
            } else {
                sb.append("\\x");
                String hexString = "00" + Integer.toHexString((int) c);
                sb.append(hexString.substring(hexString.length() - 2));
            }
        }
    }

    private void appendBytes(StringBuilder sb, byte[] bytes) {
        sb.append(new String(bytes));
    }

    public void logEndIterator(long time) {
        queryLogger.trace("accumulo iterator closed (time %dms)", time);
    }
}
