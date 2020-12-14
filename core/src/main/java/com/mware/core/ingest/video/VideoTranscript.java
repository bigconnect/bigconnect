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
package com.mware.core.ingest.video;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class VideoTranscript implements Serializable {
    private SortedSet<TimedText> entries = new TreeSet<TimedText>();

    public VideoTranscript() {

    }

    public VideoTranscript(JSONObject json) {
        try {
            JSONArray entriesJson = (JSONArray) json.get("entries");
            for (int i = 0; i < entriesJson.length(); i++) {
                JSONObject entryJson = (JSONObject) entriesJson.get(i);
                entries.add(new TimedText(entryJson));
            }
        } catch (JSONException e) {
            throw new RuntimeException("Could not parse video transcript JSON", e);
        }
    }

    public List<TimedText> getEntries() {
        return new ArrayList<TimedText>(entries);
    }

    public void add(Time time, String text) {
        entries.add(new TimedText(time, text));
    }

    public VideoTranscript merge(VideoTranscript videoTranscript) {
        for (TimedText entry : videoTranscript.entries) {
            entries.add(entry);
        }
        return this;
    }

    public static VideoTranscript merge(VideoTranscript a, VideoTranscript b) {
        VideoTranscript result = new VideoTranscript();
        if (a != null) {
            result.merge(a);
        }
        if (b != null) {
            result.merge(b);
        }
        return result;
    }

    public JSONObject toJson() {
        try {
            JSONObject result = new JSONObject();
            JSONArray entriesJson = new JSONArray();
            for (TimedText entry : entries) {
                entriesJson.put(entry.toJson());
            }
            result.put("entries", entriesJson);
            return result;
        } catch (JSONException e) {
            throw new RuntimeException("Could not create JSON", e);
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (TimedText entry : entries) {
            result.append(entry.getTime().toString());
            result.append("\n");
            result.append(entry.getText());
            result.append("\n");
        }
        return result.toString();
    }

    public long getDuration() {
        if (getEntries().size() > 0) {
            long start = getEntries().get(0).getTime().getStart();
            long end = getEntries().get(getEntries().size() - 1).getTime().getEnd();
            return end - start;
        }
        return 0;
    }

    public Integer findEntryIndexFromStartTime(long frameStartTime) {
        int entryIndex = 0;
        for (TimedText entry : getEntries()) {
            if (entry.getTime().getStart() == frameStartTime) {
                return entryIndex;
            }
            entryIndex++;
        }
        return null;
    }

    public static class TimedText implements Comparable<TimedText>, Serializable {
        private Time time;
        private String text;

        public TimedText(Time time, String text) {
            this.time = time;
            this.text = text;
        }

        public TimedText(JSONObject json) {
            try {
                Long start = null;
                Long end = null;

                if (json.has("start")) {
                    start = json.getLong("start");
                }
                if (json.has("end")) {
                    end = json.getLong("end");
                }
                this.time = new Time(start, end);
                this.text = json.getString("text");
            } catch (JSONException e) {
                throw new RuntimeException("Could not parse TimedText JSON", e);
            }
        }

        public Time getTime() {
            return time;
        }

        public String getText() {
            return text;
        }

        public JSONObject toJson() {
            try {
                JSONObject result = new JSONObject();
                if (getTime().getStart() != null) {
                    result.put("start", getTime().getStart().longValue());
                }
                if (getTime().getEnd() != null) {
                    result.put("end", getTime().getEnd().longValue());
                }
                result.put("text", getText());
                return result;
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int compareTo(TimedText other) {
            Time time = getTime();
            Time timeOther = other.getTime();
            int result = time.getStart().compareTo(timeOther.getStart());
            if (result != 0) {
                return result;
            }
            if (time.getEnd() == null && timeOther.getEnd() == null) {
                return 0;
            }
            if (time.getEnd() == null) {
                return -1;
            }
            if (timeOther.getEnd() == null) {
                return 1;
            }
            return time.getEnd().compareTo(timeOther.getEnd());
        }
    }

    public static class Time implements Serializable {
        private Long start;
        private Long end;

        /**
         * @param start time in milliseconds
         * @param end   time in milliseconds
         */
        public Time(Long start, Long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return toString(start) + "-" + toString(end);
        }

        private String toString(Long t) {
            if (t == null) {
                return "";
            }
            return String.format("%02d:%02d:%02d.%03d",
                    t / 60 / 60 / 1000,
                    (t / 60 / 1000) % 60,
                    (t / 1000) % 60,
                    t % 1000);
        }

        public Long getStart() {
            return start;
        }

        public Long getEnd() {
            return end;
        }
    }
}
