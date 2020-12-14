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
package com.mware.core.model.properties;

import com.mware.core.util.RowKeyHelper;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;

public class ArtifactDetectedObject {
    private final String edgeId;
    private final String resolvedVertexId;
    private final String originalPropertyKey;
    private String concept;
    private double x1;
    private double y1;
    private double x2;
    private double y2;
    private String process;

    public ArtifactDetectedObject(JSONObject json) {
        this.x1 = json.getDouble("x1");
        this.y1 = json.getDouble("y1");
        this.x2 = json.getDouble("x2");
        this.y2 = json.getDouble("y2");
        this.process = json.optString("process");
        this.concept = json.optString("concept");
        this.edgeId = json.optString("edgeId");
        this.resolvedVertexId = json.optString("resolvedVertexId");
        this.originalPropertyKey = json.optString("originalPropertyKey");
    }

    public ArtifactDetectedObject(double x1, double y1, double x2, double y2, String concept, String process, String edgeId, String resolvedVertexId, String originalPropertyKey) {
        this.x1 = x1;
        this.y1 = y1;
        this.x2 = x2;
        this.y2 = y2;
        this.concept = concept;
        this.process = process;
        this.edgeId = edgeId;
        this.resolvedVertexId = resolvedVertexId;
        this.originalPropertyKey = originalPropertyKey;
    }

    public ArtifactDetectedObject(double x1, double y1, double x2, double y2, String concept, String process) {
        this(x1, y1, x2, y2, concept, process, null, null, null);
    }

    public double getX1() {
        return x1;
    }

    public void setX1(double x1) {
        this.x1 = x1;
    }

    public double getY1() {
        return y1;
    }

    public void setY1(double y1) {
        this.y1 = y1;
    }

    public double getX2() {
        return x2;
    }

    public void setX2(double x2) {
        this.x2 = x2;
    }

    public double getY2() {
        return y2;
    }

    public void setY2(double y2) {
        this.y2 = y2;
    }

    public String getConcept() {
        return concept;
    }

    public void setConcept(String concept) {
        this.concept = concept;
    }

    public String getProcess() {
        return process;
    }

    public void setProcess(String process) {
        this.process = process;
    }

    public String getEdgeId() {
        return edgeId;
    }

    public String getResolvedVertexId() {
        return resolvedVertexId;
    }

    public String getOriginalPropertyKey() {
        return originalPropertyKey;
    }

    public String getMultivalueKey(String multiValueKeyPrefix) {
        return multiValueKeyPrefix
                + ":"
                + StringUtils.leftPad(Double.toString(getX1()), RowKeyHelper.OFFSET_WIDTH, '0')
                + ":"
                + StringUtils.leftPad(Double.toString(getY1()), RowKeyHelper.OFFSET_WIDTH, '0')
                + ":"
                + StringUtils.leftPad(Double.toString(getX2()), RowKeyHelper.OFFSET_WIDTH, '0')
                + ":"
                + StringUtils.leftPad(Double.toString(getY2()), RowKeyHelper.OFFSET_WIDTH, '0');
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("x1", getX1());
        json.put("y1", getY1());
        json.put("x2", getX2());
        json.put("y2", getY2());
        if (getProcess() != null) {
            json.put("process", getProcess());
        }
        if (getConcept() != null) {
            json.put("concept", getConcept());
        }
        if (getEdgeId() != null) {
            json.put("edgeId", getEdgeId());
        }
        if (getResolvedVertexId() != null) {
            json.put("resolvedVertexId", getResolvedVertexId());
        }
        if (getOriginalPropertyKey() != null) {
            json.put("originalPropertyKey", getOriginalPropertyKey());
        }
        return json;
    }
}
