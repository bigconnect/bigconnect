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

import com.mware.ge.Element;
import com.mware.ge.event.*;

public class PropertyEvent {
    public static final String ADD_PROPERTY_EVENT_TYPE = "Add Property";
    public static final String DELETE_PROPERTY_EVENT_TYPE = "Delete Property";
    public static final String MARK_HIDDEN_PROPERTY_EVENT_TYPE = "Mark Hidden Property";
    public static final String MARK_VISIBLE_PROPERTY_EVENT_TYPE = "Mark Visible Property";
    public static final String SOFT_DELETE_PROPERTY_EVENT_TYPE = "Soft Delete Property";

    private GraphEvent event;
    private String eventType;
    private boolean valid;

    private Element element;
    private String propertyName;

    public PropertyEvent(GraphEvent event) {
        this.event = event;

        buildEvent();
    }

    private void buildEvent() {
        if (this.event == null) {
            return;
        }

        setValid(true);
        //All property related events
        if (this.event instanceof AddPropertyEvent) {
            //AddPropertyEvent is used for updates also
            setEventType(ADD_PROPERTY_EVENT_TYPE);
            AddPropertyEvent _event = (AddPropertyEvent)event;
            setElement(_event.getElement());
            setPropertyName(_event.getProperty().getName());
        } else if (this.event instanceof DeletePropertyEvent) {
            setEventType(DELETE_PROPERTY_EVENT_TYPE);
            DeletePropertyEvent _event = (DeletePropertyEvent)event;
            setElement(_event.getElement());
            setPropertyName(_event.getName());
        } else if (this.event instanceof MarkHiddenPropertyEvent) {
            setEventType(MARK_HIDDEN_PROPERTY_EVENT_TYPE);
            MarkHiddenPropertyEvent _event = (MarkHiddenPropertyEvent)event;
            setElement(_event.getElement());
            setPropertyName(_event.getProperty().getName());
        } else if (this.event instanceof MarkVisiblePropertyEvent) {
            setEventType(MARK_VISIBLE_PROPERTY_EVENT_TYPE);
            MarkVisiblePropertyEvent _event = (MarkVisiblePropertyEvent)event;
            setElement(_event.getElement());
            setPropertyName(_event.getProperty().getName());
        } else if (this.event instanceof SoftDeletePropertyEvent) {
            setEventType(SOFT_DELETE_PROPERTY_EVENT_TYPE);
            SoftDeletePropertyEvent _event = (SoftDeletePropertyEvent)event;
            setElement(_event.getElement());
            setPropertyName(_event.getName());
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

    public Element getElement() {
        return element;
    }

    public void setElement(Element element) {
        this.element = element;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }
}
