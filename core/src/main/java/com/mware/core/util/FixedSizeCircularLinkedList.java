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
package com.mware.core.util;

import com.mware.core.exception.BcException;

import java.util.ArrayList;
import java.util.List;

public class FixedSizeCircularLinkedList<T> {
    private Node<T> head;

    public FixedSizeCircularLinkedList(int size, Class<T> type) {
        Node<T> first = head = new Node<T>(0, null, type);
        for (int i = 1; i < size; i++) {
            head = new Node<T>(i, head, type);
        }
        head.setNext(first.setPrevious(head));
        head = first;
    }

    public T head() {
        return head.getData();
    }

    public void rotateForward() {
        head = head.getNext();
    }

    public List<T> readBackward(int count) {
        List<T> data = new ArrayList<T>(count);
        Node<T> node = head.getPrevious();
        for (int i = 0; i < count; i++) {
            data.add(node.getData());
            node = node.getPrevious();
        }
        return data;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for(Node<T> node = head; first || node != head; node = node.getNext()) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(node).append(":").append(node.getData());
        }
        return sb.toString();
    }

    private class Node<t> {
        private Node<t> previous;
        private Node<t> next;
        private int id;
        private t data;

        protected Node(int id, Node<t> previous, Class<t> type) {
            this.id = id;
            if (previous != null) {
                this.previous = previous;
                previous.setNext(this);
            }
            try {
                data = type.newInstance();
            } catch (IllegalAccessException iae) {
                throw new BcException("error creating new instance of type", iae);
            } catch (InstantiationException ie) {
                throw new BcException("error creating new instance of type", ie);
            }
        }

        protected Node<t> setPrevious(Node<t> previous) {
            this.previous = previous;
            return this;
        }

        protected Node<t> setNext(Node<t> next) {
            this.next = next;
            return this;
        }

        protected Node<t> getPrevious() {
            return previous;
        }

        protected Node<t> getNext() {
            return next;
        }

        protected t getData() {
            return data;
        }

        public String toString() {
            return Integer.toString(id);
        }
    }
}
