package io.bigconnect.biggraph.core;

import io.bigconnect.biggraph.BigGraph;
import io.bigconnect.biggraph.schema.SchemaManager;
import io.bigconnect.biggraph.testutil.Assert;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

public class VertexCoreTest extends BaseCoreTest {
    @Before
    public void initSchema() {
        SchemaManager schema = graph().schema();

        LOG.debug("===============  propertyKey  ================");

        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("dynamic").asBoolean().create();
        schema.propertyKey("time").asText().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("comment").asText().valueList().create();
        schema.propertyKey("contribution").asText().valueSet().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("description").asText().create();
        schema.propertyKey("city").asText().create();
        schema.propertyKey("cpu").asText().create();
        schema.propertyKey("ram").asText().create();
        schema.propertyKey("band").asText().create();
        schema.propertyKey("price").asInt().create();
        schema.propertyKey("weight").asDouble().create();
        schema.propertyKey("birth").asDate().create();

        LOG.debug("===============  vertexLabel  ================");

        schema.vertexLabel("person")
                .properties("name", "age", "city", "birth")
                .primaryKeys("name")
                .nullableKeys("age", "birth")
                .create();
        schema.vertexLabel("computer")
                .properties("name", "band", "cpu", "ram", "price")
                .primaryKeys("name", "band")
                .nullableKeys("ram", "cpu", "price")
                .ifNotExist()
                .create();
        schema.vertexLabel("author")
                .properties("id", "name", "age", "lived")
                .primaryKeys("id")
                .nullableKeys("age", "lived")
                .create();
        schema.vertexLabel("language")
                .properties("name", "dynamic")
                .primaryKeys("name")
                .nullableKeys("dynamic")
                .create();
        schema.vertexLabel("book")
                .properties("name", "price")
                .primaryKeys("name")
                .nullableKeys("price")
                .create();
        schema.vertexLabel("review")
                .properties("id", "comment", "contribution")
                .primaryKeys("id")
                .nullableKeys("comment", "contribution")
                .create();
        schema.vertexLabel("fan")
                .properties("name", "age", "city")
                .primaryKeys("name")
                .ttl(3000L)
                .ifNotExist()
                .create();
        schema.vertexLabel("follower")
                .properties("name", "age", "city", "birth")
                .primaryKeys("name")
                .ttl(3000L)
                .ttlStartTime("birth")
                .ifNotExist()
                .create();
    }

    @Test
    public void testAddVertex() {
        BigGraph graph = graph();

        // Directly save into the backend
        Vertex v = graph.addVertex(T.label, "book", "name", "java-3");
        graph.tx().commit();

        long count = graph.traversal().V().count().next();
        Assert.assertEquals(1, count);
    }
}
