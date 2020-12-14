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
package com.mware.core.ingest.dataworker;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.termMention.TermMentionBuilder;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;
import com.mware.ge.Element;
import com.mware.ge.Property;
import com.mware.ge.Vertex;
import com.mware.ge.values.storable.TextValue;
import com.mware.ge.values.storable.Value;
import com.mware.ge.values.storable.Values;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class RegexDataWorker extends DataWorker {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(RegexDataWorker.class);
    private final Pattern pattern;

    public RegexDataWorker(String regEx) {
        this.pattern = Pattern.compile(regEx, Pattern.MULTILINE);
    }

    protected abstract Concept getConcept();

    @Override
    public void prepare(DataWorkerPrepareData workerPrepareData) throws Exception {
        super.prepare(workerPrepareData);
        LOGGER.debug("Extractor prepared for entity type [%s] with regular expression: %s", getConcept().getName(), this.pattern.toString());
    }

    @Override
    public void execute(InputStream in, DataWorkerData data) throws Exception {
        LOGGER.debug("Extracting pattern [%s] from provided text", pattern);

        final String text = CharStreams.toString(new InputStreamReader(in, Charsets.UTF_8));

        final Matcher matcher = pattern.matcher(text);

        Vertex outVertex = (Vertex) data.getElement();

        List<Vertex> termMentions = new ArrayList<>();
        while (matcher.find()) {
            final String patternGroup = matcher.group();
            int start = matcher.start();
            int end = matcher.end();

            Vertex termMention = new TermMentionBuilder()
                    .outVertex(outVertex)
                    .propertyKey(data.getProperty().getKey())
                    .propertyName(data.getProperty().getName())
                    .start(start)
                    .end(end)
                    .title(patternGroup)
                    .conceptName(getConcept().getName())
                    .visibilityJson(data.getElementVisibilityJson())
                    .process(getClass().getName())
                    .save(getGraph(), getVisibilityTranslator(), getUser(), getAuthorizations());
            termMentions.add(termMention);
        }
        applyTermMentionFilters(outVertex, termMentions);
        pushTextUpdated(data);
    }

    @Override
    public boolean isHandled(Element element, Property property) {
        if (property == null) {
            return false;
        }

        if (property.getName().equals(BcSchema.RAW.getPropertyName())) {
            return false;
        }

        Value mimeType = property.getMetadata().getValue(BcSchema.MIME_TYPE.getPropertyName());
        return !(mimeType.eq(Values.NO_VALUE) || !((TextValue)mimeType).stringValue().startsWith("text"));
    }
}
