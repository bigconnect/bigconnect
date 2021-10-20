package io.bigconnect.biggraph.job.schema;

import io.bigconnect.biggraph.backend.id.Id;
import io.bigconnect.biggraph.backend.id.IdGenerator;
import io.bigconnect.biggraph.backend.tx.SchemaTransaction;
import io.bigconnect.biggraph.job.SysJob;
import io.bigconnect.biggraph.schema.IndexLabel;
import io.bigconnect.biggraph.schema.SchemaElement;
import io.bigconnect.biggraph.schema.SchemaLabel;
import io.bigconnect.biggraph.type.BigType;
import io.bigconnect.biggraph.util.E;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public abstract class SchemaCallable extends SysJob<Object> {

    public static final String REMOVE_SCHEMA = "remove_schema";
    public static final String REBUILD_INDEX = "rebuild_index";
    public static final String CREATE_INDEX = "create_index";
    public static final String CREATE_OLAP = "create_olap";
    public static final String CLEAR_OLAP = "clear_olap";
    public static final String REMOVE_OLAP = "remove_olap";

    private static final String SPLITOR = ":";

    protected BigType schemaType() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR, 3);
        E.checkState(parts.length == 3 && parts[0] != null,
                     "Task name should be formatted to String " +
                     "'TYPE:ID:NAME', but got '%s'", name);

        return BigType.valueOf(parts[0]);
    }

    protected Id schemaId() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR, 3);
        E.checkState(parts.length == 3 && parts[1] != null,
                     "Task name should be formatted to String " +
                     "'TYPE:ID:NAME', but got '%s'", name);
        return IdGenerator.of(Long.valueOf(parts[1]));
    }

    protected String schemaName() {
        String name = this.task().name();
        String[] parts = name.split(SPLITOR, 3);
        E.checkState(parts.length == 3 && parts[2] != null,
                     "Task name should be formatted to String " +
                     "'TYPE:ID:NAME', but got '%s'", name);
        return parts[2];
    }

    public static String formatTaskName(BigType type, Id id, String name) {
        E.checkNotNull(type, "schema type");
        E.checkNotNull(id, "schema id");
        E.checkNotNull(name, "schema name");
        return String.join(SPLITOR, type.toString(), id.asString(), name);
    }

    protected static void removeIndexLabelFromBaseLabel(SchemaTransaction tx,
                                                        IndexLabel label) {
        BigType baseType = label.baseType();
        Id baseValue = label.baseValue();
        SchemaLabel schemaLabel;
        if (baseType == BigType.VERTEX_LABEL) {
            if (SchemaElement.OLAP_ID.equals(baseValue)) {
                return;
            }
            schemaLabel = tx.getVertexLabel(baseValue);
        } else {
            assert baseType == BigType.EDGE_LABEL;
            schemaLabel = tx.getEdgeLabel(baseValue);
        }
        assert schemaLabel != null;
        schemaLabel.removeIndexLabel(label.id());
        updateSchema(tx, schemaLabel);
    }

    /**
     * Use reflection to call SchemaTransaction.removeSchema(),
     * which is protected
     * @param tx        The remove operation actual executer
     * @param schema    the schema to be removed
     */
    protected static void removeSchema(SchemaTransaction tx,
                                       SchemaElement schema) {
        try {
            Method method = SchemaTransaction.class
                            .getDeclaredMethod("removeSchema",
                                               SchemaElement.class);
            method.setAccessible(true);
            method.invoke(tx, schema);
        } catch (NoSuchMethodException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new AssertionError(
                      "Can't call SchemaTransaction.removeSchema()", e);
        }

    }

    /**
     * Use reflection to call SchemaTransaction.updateSchema(),
     * which is protected
     * @param tx        The update operation actual executer
     * @param schema    the schema to be update
     */
    protected static void updateSchema(SchemaTransaction tx,
                                       SchemaElement schema) {
        try {
            Method method = SchemaTransaction.class
                            .getDeclaredMethod("updateSchema",
                                               SchemaElement.class);
            method.setAccessible(true);
            method.invoke(tx, schema);
        } catch (NoSuchMethodException | IllegalAccessException |
                 InvocationTargetException e) {
            throw new AssertionError(
                      "Can't call SchemaTransaction.updateSchema()", e);
        }
    }
}
