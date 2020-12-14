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
package com.mware.ge;

public class FindPathOptions {
    private final String sourceVertexId;
    private final String destVertexId;
    private final int maxHops;
    private String[] labels;
    private String[] excludedLabels;
    private ProgressCallback progressCallback;
    private boolean getAnyPath;

    /**
     * @param sourceVertexId The source vertex id to start the search from.
     * @param destVertexId   The destination vertex id to get to.
     * @param maxHops        The maximum number of hops to make before giving up.
     */
    public FindPathOptions(String sourceVertexId, String destVertexId, int maxHops) {
        this.sourceVertexId = sourceVertexId;
        this.destVertexId = destVertexId;
        this.maxHops = maxHops;
        this.getAnyPath = false;
    }

    /**
     * @param sourceVertexId The source vertex id to start the search from.
     * @param destVertexId   The destination vertex id to get to.
     * @param maxHops        The maximum number of hops to make before giving up.
     * @param getAnyPath     Return as soon as the first path is found
     */
    public FindPathOptions(String sourceVertexId, String destVertexId, int maxHops, boolean getAnyPath) {
        this.sourceVertexId = sourceVertexId;
        this.destVertexId = destVertexId;
        this.maxHops = maxHops;
        this.getAnyPath = getAnyPath;
    }

    public String getSourceVertexId() {
        return sourceVertexId;
    }

    public String getDestVertexId() {
        return destVertexId;
    }

    public int getMaxHops() {
        return maxHops;
    }

    public String[] getLabels() {
        return labels;
    }

    public boolean isGetAnyPath() {
        return getAnyPath;
    }

    /**
     * Edge labels to include, if null any label will be traversed
     */
    public FindPathOptions setLabels(String... labels) {
        this.labels = labels;
        return this;
    }

    public String[] getExcludedLabels() {
        return excludedLabels;
    }

    /**
     * Edge labels to be excluded from traversal
     */
    public FindPathOptions setExcludedLabels(String... excludedLabels) {
        this.excludedLabels = excludedLabels;
        return this;
    }

    public ProgressCallback getProgressCallback() {
        return progressCallback;
    }

    public FindPathOptions setProgressCallback(ProgressCallback progressCallback) {
        this.progressCallback = progressCallback;
        return this;
    }

    @Override
    public String toString() {
        return "FindPathOptions{" +
                "sourceVertexId='" + sourceVertexId + '\'' +
                ", destVertexId='" + destVertexId + '\'' +
                ", maxHops=" + maxHops +
                '}';
    }
}