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
package com.mware.core.watcher;

import com.mware.ge.Edge;
import com.mware.ge.event.*;

/**
 * Created by Dan on 12/22/2016.
 */
public class EdgeEvent {
    public static final String ADD_EDGE_EVENT_TYPE = "Add Relationship";
    public static final String SOFT_DELETE_EDGE_EVENT_TYPE = "Soft Delete Relationship";

    private GraphEvent event;
    private String eventType;
    private boolean valid;

    private Edge edge;
    private String title;

    public EdgeEvent(GraphEvent event) {
        this.event = event;

        buildEvent();
    }

    private void buildEvent() {
        if (this.event == null) {
            return;
        }

        setValid(true);
        //All relationship related events
        if (this.event instanceof AddEdgeEvent) {
            //AddEdgeEvent is used for updates also
            setEventType(ADD_EDGE_EVENT_TYPE);
            AddEdgeEvent _event = (AddEdgeEvent)event;
            setEdge(_event.getEdge());
            setTitle(_event.getEdge().getLabel());
        } else if (this.event instanceof SoftDeleteEdgeEvent) {
            setEventType(SOFT_DELETE_EDGE_EVENT_TYPE);
            SoftDeleteEdgeEvent _event = (SoftDeleteEdgeEvent)event;
            setEdge(_event.getEdge());
            setTitle(_event.getEdge().getLabel());
        } else {
            setValid(false);
        }
    }

    public GraphEvent getEvent() {
        return event;
    }

    public void setEvent(GraphEvent event) {
        this.event = event;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public Edge getEdge() {
        return edge;
    }

    public void setEdge(Edge edge) {
        this.edge = edge;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}