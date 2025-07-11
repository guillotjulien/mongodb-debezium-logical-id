package com.github.guillotjulien;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * This Kafka Connect SMT handles the extraction of a value from a message in
 * order to use it as key.
 * 
 * When the operation is a deletion, the message will automatically be converted
 * into a tombstone after extracting the key. 
 * This functionality requires an operation type key to be defined in the document.
 */
public class GenerateTombstoneWithId<R extends ConnectRecord<R>> implements Transformation<R> {
    public interface ConfigName {
        String ID_FIELD_NAME = "id.field.name";
        String OP_FIELD_NAME = "op.field.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.ID_FIELD_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Field name for the ID of the document")
            .define(ConfigName.OP_FIELD_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Field name for the Debezium operation type");

    private String idFieldName;
    private ExtractField<R> idExtractor;
    private ExtractField<R> opExtractor;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        idFieldName = config.getString(ConfigName.ID_FIELD_NAME);

        idExtractor = extractValueDelegate(idFieldName);
        opExtractor = extractValueDelegate(config.getString(ConfigName.OP_FIELD_NAME));
    }

    @Override
    public R apply(R record) {
        // Handling tombstone record
        if (record.value() == null) {
            return record;
        }

        final R idRecord = idExtractor.apply(record);
        if (idRecord.value() == null) {
            throw new DataException("Field does not exist: " + idFieldName);
        }

        final R opRecord = opExtractor.apply(record);
        if (opRecord.value() == null) {
            throw new DataException("Field does not exist: op");
        }

        final Schema keySchema = SchemaBuilder.struct()
                .field(idFieldName, idRecord.valueSchema())
                .build();

        final Struct key = new Struct(keySchema).put(idFieldName, idRecord.value());

        // No need to touch the record when the operation is not a deletion
        if (opRecord.value().equals("d")) {
            return record.newRecord(
                    record.topic(),
                    null, // New partition should be determined based on the new key
                    keySchema,
                    key,
                    null,
                    null, // We want the message to be converted into a tombstone
                    record.timestamp());
        }

        return record.newRecord(
                record.topic(),
                null,
                keySchema,
                key,
                record.valueSchema(),
                record.value(),
                record.timestamp());
    }

    @Override
    public void close() {
        idExtractor.close();
        opExtractor.close();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    private <R extends ConnectRecord<R>> ExtractField<R> extractValueDelegate(String field) {
        ExtractField<R> extractField = new ExtractField.Value<>();
        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", field);
        extractField.configure(delegateConfig);
        return extractField;
    }
}
