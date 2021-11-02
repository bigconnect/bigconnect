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
package com.mware.ge.cypher.builtin.proc;

import com.mware.core.ingest.dataworker.ElementOrPropertyStatus;
import com.mware.core.model.clientapi.dto.VisibilityJson;
import com.mware.core.model.graph.GraphRepository;
import com.mware.core.model.graph.GraphUpdateContext;
import com.mware.core.model.properties.ArtifactDetectedObject;
import com.mware.core.model.properties.ArtifactDetectedObjectMetadataBcProperty;
import com.mware.core.model.properties.BcSchema;
import com.mware.core.model.properties.RawObjectSchema;
import com.mware.core.model.properties.types.PropertyMetadata;
import com.mware.core.model.schema.Concept;
import com.mware.core.model.schema.SchemaConstants;
import com.mware.core.model.schema.SchemaRepository;
import com.mware.core.model.termMention.TermMentionRepository;
import com.mware.core.model.termMention.TermMentionUtils;
import com.mware.core.model.workQueue.Priority;
import com.mware.core.model.workQueue.WorkQueueRepository;
import com.mware.core.security.VisibilityTranslator;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.ge.*;
import com.mware.ge.dependencies.DependencyResolver;
import com.mware.ge.cypher.exception.ProcedureException;
import com.mware.ge.cypher.exception.Status;
import com.mware.ge.cypher.ge.GeCypherQueryContext;
import com.mware.ge.cypher.procedure.Context;
import com.mware.ge.cypher.procedure.Description;
import com.mware.ge.cypher.procedure.Name;
import com.mware.ge.cypher.procedure.Procedure;
import com.mware.ge.values.storable.ByteArray;
import com.mware.ge.values.storable.DefaultStreamingPropertyValue;
import com.mware.ge.values.storable.StreamingPropertyValue;
import com.mware.ge.values.storable.Values;
import org.apache.commons.lang3.StringUtils;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

import static com.mware.core.ingest.dataworker.MimeTypeDataWorker.MULTI_VALUE_KEY;
import static com.mware.core.model.schema.SchemaConstants.EDGE_LABEL_HAS_IMAGE;
import static com.mware.ge.dependencies.DependencyResolver.SelectionStrategy.ONLY;

public class DataWorkerProcedures {
    @Context
    public GeCypherQueryContext bcApi;

    @Context
    public DependencyResolver dependencies;

    @Procedure(name = "dw.pushOnQueue")
    @Description("dw.pushOnQueue(node|edge, propKey, propName).")
    public void pushOnQueue(@Name("element") Element elem,
                            @Name(value = "key", defaultValue = "") String propertyKey,
                            @Name(value = "name", defaultValue = "") String propertyName) {
        if (bcApi.getWorkQueueRepository() != null) {
            if (StringUtils.isEmpty(propertyName)) {
                bcApi.getWorkQueueRepository().pushOnDwQueue(
                        elem,
                        null,
                        null,
                        bcApi.getWorkspaceId(),
                        null,
                        Priority.NORMAL,
                        ElementOrPropertyStatus.UPDATE,
                        null
                );
            } else {
                Iterable<Property> properties = elem.getProperties(propertyName);
                for (Property property : properties) {
                    bcApi.getWorkQueueRepository().pushOnDwQueue(
                            elem,
                            property.getKey(),
                            property.getName(),
                            bcApi.getWorkspaceId(),
                            null,
                            Priority.NORMAL,
                            ElementOrPropertyStatus.UPDATE,
                            null
                    );
                }
            }
        } else {
            throw new ProcedureException(Status.Procedure.ProcedureCallFailed, "The Work Queue Repository is not available ");
        }
    }

    @Procedure(name = "dw.createTermMention")
    @Description("dw.createTermMention(node, propertyKey, tmName, tmStart, tmEnd, tmConcept, remove, resolve).")
    public void createTermMention(
            @Name("node") Vertex node,
            @Name("tmName") String tmName,
            @Name("tmStart") Number tmStart,
            @Name("tmEnd") Number tmEnd,
            @Name("tmConcept") String tmConcept,
            @Name(value = "tmPropertyKey", defaultValue = "") String tmPropertyKey,
            @Name(value = "remove", defaultValue = "true") Boolean remove,
            @Name(value = "resolve", defaultValue = "false") Boolean resolve
    ) {
        TermMentionUtils termMentionUtils = new TermMentionUtils(
                bcApi.getGraph(),
                dependencies.resolveDependency(VisibilityTranslator.class, ONLY),
                bcApi.getAuthorizations(),
                new SystemUser()
        );

        TermMentionRepository termMentionRepository = dependencies.resolveDependency(TermMentionRepository.class, ONLY);

        if (remove) {
            termMentionUtils.removeHasDetectedEntityRelations(node);
            termMentionRepository.deleteTermMentions("ent", node.getId(), bcApi.getAuthorizations());
            //backwards compatibility
            termMentionRepository.deleteTermMentions("", node.getId(), bcApi.getAuthorizations());
        }


        Vertex tm = termMentionUtils.createTermMention(
                node,
                tmPropertyKey,
                BcSchema.TEXT.getPropertyName(),
                tmName,
                tmConcept,
                tmStart.intValue(),
                tmEnd.intValue(),
                BcSchema.VISIBILITY_JSON.getPropertyValue(node)
        );
        bcApi.getGraph().flush();

        if (resolve) {
            List<Element> resolvedVertices = termMentionUtils.resolveTermMentions(node, Collections.singletonList(tm));

            WorkQueueRepository workQueueRepository = bcApi.getWorkQueueRepository();
            workQueueRepository.pushMultipleElementOnDwQueue(
                    resolvedVertices,
                    "NLP",
                    BcSchema.TITLE.getPropertyName(),
                    null,
                    null,
                    Priority.HIGH,
                    ElementOrPropertyStatus.UPDATE,
                    null
            );
        }

    }

    @Procedure(name = "dw.createObject")
    @Description("dw.createObject(node, x1, y1, x2, y2, probability, class).")
    public void createObject(
            @Name("node") Vertex node,
            @Name("x1") Number x1,
            @Name("y1") Number y1,
            @Name("x2") Number x2,
            @Name("y2") Number y2,
            @Name("probability") Number _confidence,
            @Name("class") String clazz) {
        synchronized (bcApi.getGraph()) {
            SchemaRepository schemaRepository = bcApi.getSchemaRepository();
            GraphRepository graphRepository = dependencies.resolveDependency(GraphRepository.class, ONLY);

            String artifactContainsImageOfEntityIri =
                    schemaRepository.getRequiredRelationshipNameByIntent(
                            "artifactContainsImageOfEntity", SchemaRepository.PUBLIC);
            User user = new SystemUser();
            ZonedDateTime modifiedDate = ZonedDateTime.now();

            PropertyMetadata propertyMetadata =
                    new PropertyMetadata(modifiedDate, user, new VisibilityJson(), Visibility.EMPTY);
            String id = bcApi.getGraph().getIdGenerator().nextId();
            Vertex artifactVertex = bcApi.getGraph().getVertex(node.getId(), bcApi.getAuthorizations());

            // Check for duplicates
            Iterable<Property> props = RawObjectSchema.DETECTED_OBJECT.getProperties(artifactVertex);
            for (Property prop : props) {
                ArtifactDetectedObject artifact = RawObjectSchema.DETECTED_OBJECT_METADATA.getMetadataValue(prop);
                // Duck typing
                if (Math.abs(artifact.getX1() - x1.doubleValue()) <= 0.01 &&
                        Math.abs(artifact.getX2() - x2.doubleValue()) <= 0.01 &&
                        Math.abs(artifact.getY1() - y1.doubleValue()) <= 0.01 &&
                        Math.abs(artifact.getY2() - y2.doubleValue()) <= 0.01 &&
                        clazz.equals(artifact.getConcept())) {
                    return;
                }
            }

            Edge edge;
            ArtifactDetectedObject artifactDetectedObject;
            try (GraphUpdateContext ctx =
                         graphRepository.beginGraphUpdate(Priority.NORMAL, user, bcApi.getAuthorizations())) {
                ctx.setPushOnQueue(false);
                // Create Edge
                edge = ctx.getOrCreateEdgeAndUpdate(null, node.getId(), id, artifactContainsImageOfEntityIri, Visibility.EMPTY, edgeCtx ->
                        edgeCtx.updateBuiltInProperties(propertyMetadata)
                ).get();

                // Create Artifact
                artifactDetectedObject = new ArtifactDetectedObject(
                        x1.doubleValue(),
                        y1.doubleValue(),
                        x2.doubleValue(),
                        y2.doubleValue(),
                        _confidence.doubleValue(),
                        clazz,
                        "system",
                        edge.getId(),
                        id,
                        null
                );

                // Create concept if it doesn't exist
                Concept concept = schemaRepository.getConceptByName(clazz, null);
                if (concept == null) {
                    bcApi.createNewConcept(clazz, null);
                }

                // Create Vertex
                final String propertyKey = artifactDetectedObject.getMultivalueKey(DataWorkerProcedures.class.getName());
                double confidence = Math.floor(_confidence.doubleValue() * 100) / 100.0;
                Vertex resolvedObjectVertex = ctx.getOrCreateVertexAndUpdate(id, Visibility.EMPTY, clazz, elemCtx -> {
                    if (elemCtx.isNewElement()) {
                        elemCtx.updateBuiltInProperties(propertyMetadata);
                        BcSchema.TITLE.updateProperty(elemCtx, MULTI_VALUE_KEY,
                                clazz + " [" + confidence + "]",
                                propertyMetadata);
                    }

                    RawObjectSchema.ROW_KEY.updateProperty(elemCtx, id, propertyKey, propertyMetadata);
                }).get();

                ctx.update(artifactVertex, elemCtx -> {
                    Metadata m = Metadata.create();
                    RawObjectSchema.DETECTED_OBJECT_METADATA.setMetadata(m, artifactDetectedObject, Visibility.EMPTY);
                    RawObjectSchema.DETECTED_OBJECT.updateProperty(elemCtx, propertyKey, artifactDetectedObject.getConcept(), propertyMetadata);
                }).get();

                createCroppedImageVertex(ctx, Visibility.EMPTY, propertyMetadata, artifactVertex, resolvedObjectVertex,
                        x1.doubleValue(), x2.doubleValue(), y1.doubleValue(), y2.doubleValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void createCroppedImageVertex(GraphUpdateContext ctx,
                                          Visibility visibility,
                                          PropertyMetadata propertyMetadata,
                                          Vertex artifactVertex,
                                          Vertex resolvedObjectVertex,
                                          double x1, double x2, double y1, double y2) {
        try (InputStream imageStream = BcSchema.RAW.getPropertyValue(artifactVertex).getInputStream()) {
            BufferedImage image = ImageIO.read(imageStream);
            double sx1 = Math.max(0, x1 * image.getWidth());
            double sy1 = Math.max(0, y1 * image.getHeight());
            double sx2 = x2 * image.getWidth();
            double sy2 = y2 * image.getHeight();
            BufferedImage cropped = image.getSubimage((int) sx1, (int) sy1,
                    (int) (Math.min(image.getWidth(), sx2) - sx1),
                    (int) (Math.min(image.getHeight(), sy2) - sy1));

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ImageIO.write(cropped, "jpeg", os);
            StreamingPropertyValue spv =
                    DefaultStreamingPropertyValue.create(new ByteArrayInputStream(os.toByteArray()), ByteArray.class);

            String id = bcApi.getGraph().getIdGenerator().nextId();
            // Cropped image vertex
            Vertex imageVertex =
                    ctx.getOrCreateVertexAndUpdate(id, visibility, SchemaConstants.CONCEPT_TYPE_IMAGE, elemCtx -> {
                        elemCtx.updateBuiltInProperties(propertyMetadata);
                        BcSchema.MIME_TYPE.updateProperty(elemCtx, "", "image/jpeg", propertyMetadata);
                        propertyMetadata.add(BcSchema.MIME_TYPE.getPropertyName(), Values.stringValue("image/jpeg"), visibility);
                        BcSchema.RAW.updateProperty(elemCtx, spv, propertyMetadata);
                    }).get();
            // Edge: resolvedObject vertex -has image-> cropped image vertex
            ctx.getOrCreateEdgeAndUpdate(
                    null, resolvedObjectVertex.getId(), id, EDGE_LABEL_HAS_IMAGE, visibility, edgeCtx -> {
                        edgeCtx.updateBuiltInProperties(propertyMetadata);
                    }).get();

            RawObjectSchema.ENTITY_IMAGE_VERTEX_ID
                    .setProperty(resolvedObjectVertex, id, visibility, bcApi.getAuthorizations());
            RawObjectSchema.IMAGE_OWNER_VERTEX_ID
                    .setProperty(imageVertex, resolvedObjectVertex.getId(), visibility, bcApi.getAuthorizations());
            RawObjectSchema.IMAGE_OWNER_CONCEPT_TYPE
                    .setProperty(imageVertex, resolvedObjectVertex.getConceptType(), visibility, bcApi.getAuthorizations());

            WorkQueueRepository workQueueRepository = bcApi.getWorkQueueRepository();
            workQueueRepository.pushOnDwQueue(
                    imageVertex,
                    "",
                    RawObjectSchema.DETECTED_OBJECT.getPropertyName(),
                    null,
                    null,
                    Priority.HIGH,
                    ElementOrPropertyStatus.UPDATE,
                    null
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
