package com.github.guillotjulien;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class GenerateTombstoneWithIdTest {

    protected GenerateTombstoneWithId<SourceRecord> transformation = new GenerateTombstoneWithId<>();

    private final String ID_FIELD = "logicalId";
    private final String OP_FIELD = "op";

    @BeforeAll
    public void configure() {
        HashMap<String, String> config = new HashMap<>();

        config.put(GenerateTombstoneWithId.ConfigName.ID_FIELD_NAME, ID_FIELD);
        config.put(GenerateTombstoneWithId.ConfigName.OP_FIELD_NAME, OP_FIELD);

        transformation.configure(config);
    }

    @AfterAll
    public void closeSmt() {
        transformation.close();
    }

    @Test
    public void shouldPassTombstoneMessages() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema).put("id", "base-id");

        final Schema valueSchema = SchemaBuilder.struct()
                .field(OP_FIELD, Schema.STRING_SCHEMA)
                .field(ID_FIELD, Schema.STRING_SCHEMA)
                .build();

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "my-topic",
                keySchema,
                key,
                valueSchema,
                null);

        SourceRecord transformed = transformation.apply(eventRecord);

        assertThat(transformed).isSameAs(eventRecord);
    }

    @Test
    public void shouldExtractLogicalId() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema).put("id", "base-id");

        final Schema valueSchema = SchemaBuilder.struct()
                .field(OP_FIELD, Schema.STRING_SCHEMA)
                .field(ID_FIELD, Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema)
                .put(OP_FIELD, "c")
                .put(ID_FIELD, "logical-id");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "my-topic",
                keySchema,
                key,
                valueSchema,
                value);

        SourceRecord transformed = transformation.apply(eventRecord);

        assertEquals("logical-id", ((Struct) transformed.key()).get(ID_FIELD));
    }

    @Test
    public void shouldExtractLogicalIdNumber() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema).put("id", "base-id");

        final Schema valueSchema = SchemaBuilder.struct()
                .field(OP_FIELD, Schema.STRING_SCHEMA)
                .field(ID_FIELD, Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema)
                .put(OP_FIELD, "c")
                .put(ID_FIELD, 1);

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "my-topic",
                keySchema,
                key,
                valueSchema,
                value);

        SourceRecord transformed = transformation.apply(eventRecord);

        assertEquals(1, ((Struct) transformed.key()).get(ID_FIELD));
    }

    @Test
    public void shouldPassMessage() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema).put("id", "base-id");

        final Schema valueSchema = SchemaBuilder.struct()
                .field(OP_FIELD, Schema.STRING_SCHEMA)
                .field(ID_FIELD, Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema)
                .put(OP_FIELD, "u")
                .put(ID_FIELD, "logical-id");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "my-topic",
                keySchema,
                key,
                valueSchema,
                value);

        SourceRecord transformed = transformation.apply(eventRecord);

        assertEquals("logical-id", ((Struct) transformed.key()).get(ID_FIELD));
        assertThat(transformed.value()).isSameAs(value);
    }

    @Test
    public void shouldConvertMessageIntoTombstoneWhenOpIsDelete() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema).put("id", "base-id");

        final Schema valueSchema = SchemaBuilder.struct()
                .field(OP_FIELD, Schema.STRING_SCHEMA)
                .field(ID_FIELD, Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema)
                .put(OP_FIELD, "d")
                .put(ID_FIELD, "logical-id");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "my-topic",
                keySchema,
                key,
                valueSchema,
                value);

        SourceRecord transformed = transformation.apply(eventRecord);

        assertThat(transformed.value()).isNull();
    }

    @Test()
    public void shouldThrowWhenOpIsNotPresent() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema).put("id", "base-id");

        final Schema valueSchema = SchemaBuilder.struct()
                .field(OP_FIELD, Schema.STRING_SCHEMA)
                .field(ID_FIELD, Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema)
                .put(ID_FIELD, "logical-id");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "my-topic",
                keySchema,
                key,
                valueSchema,
                value);

        DataException exception = assertThrows(DataException.class, () -> {
            transformation.apply(eventRecord);
        });

        String expectedMessage = "Field does not exist: op";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test()
    public void shouldThrowWhenLogicalIdIsNotPresent() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema).put("id", "base-id");

        final Schema valueSchema = SchemaBuilder.struct()
                .field(OP_FIELD, Schema.STRING_SCHEMA)
                .field(ID_FIELD, Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema)
                .put(OP_FIELD, "c");

        final SourceRecord eventRecord = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "my-topic",
                keySchema,
                key,
                valueSchema,
                value);

        DataException exception = assertThrows(DataException.class, () -> {
            transformation.apply(eventRecord);
        });

        String expectedMessage = "Field does not exist: " + ID_FIELD;
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }
}
