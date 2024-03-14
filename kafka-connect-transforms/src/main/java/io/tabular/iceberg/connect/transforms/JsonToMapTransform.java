package io.tabular.iceberg.connect.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.*;
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.NodeType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Locale;
import java.util.Map.Entry;

import java.util.Map;

public class JsonToMapTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String JSON_LEVEL = "transforms.json.level";

    private static final ObjectReader mapper = new ObjectMapper().reader();

    private int JSON_LEVEL_VALUE;

    public static final ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            JSON_LEVEL,
                            ConfigDef.Type.INT,
                            1,
                            ConfigDef.Importance.MEDIUM,
                            "Positive value at which level in the json to convert to Map<String, String>");
    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        } else {
            return process(record);
        }
    }

    private R process(R record) {
        if (!(record.value() instanceof String)) {
            // filter out non-string messages
            return newNullRecord(record);
        }

        String json = (String) record.value();
        JsonNode obj;
        try {
            obj = mapper.readTree(json);
        } catch (Exception unused) {
            return newNullRecord(record);
        }

        if (!(obj instanceof ObjectNode)) {
            return newNullRecord(record);
        }

        return record;
    }

    private R walk(R record, ObjectNode node) {

        SchemaBuilder builder = SchemaBuilder.struct();

        node.fields().forEachRemaining(a ->{
          String name = a.getKey();
          JsonNode currentNode = a.getValue();

          if (!(currentNode instanceof ObjectNode)) {

            }
        }

    }

    public void addFieldSchema(Entry<String, JsonNode> kv, SchemaBuilder builder) {
        String key = kv.getKey();
        JsonNode value = kv.getValue();
        if (value.isNull()) {

        } else if (value.isInt()) {
            builder.field(key, SchemaBuilder.OPTIONAL_INT32_SCHEMA);
        } else if (value.isLong()) {
            builder.field(key, SchemaBuilder.OPTIONAL_INT64_SCHEMA);
        } else if (value.isTextual()) {
            builder.field(key, SchemaBuilder.OPTIONAL_STRING_SCHEMA);
        } else if (value.isFloat()) {
            builder.field(key, SchemaBuilder.OPTIONAL_FLOAT32_SCHEMA);
        } else if (value.isDouble()) {
            builder.field(key, SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA);
        } else if (value.isBoolean()) {
            builder.field(key, SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA);
        } else if (value.isMissingNode()) {
            // not sure that this can exist
        } else if (value.isObject()) {
            // nested time
        } else if (value.isArray()) {
            // array time, need to confirm all records are of the same type.
            ArrayNode arr = (ArrayNode) value;
            if (!arr.isEmpty()) {
                JsonNodeType arrType = null;
                nodeType = arr.elements().forEachRemaining();
                builder.field(key, SchemaBuilder.array(arr.values).optional().build());
            }
        }
        else if (value.isBigInteger()) {
            // not sure, convert to string?
        } else if (value.isBigDecimal()) {
            // not sure, convert to string?
        } else if (value.isBinary()) {
            // is this a base64 or base64 URL encoded string>
            builder.field(key, SchemaBuilder.BYTES_SCHEMA);
            // not sure, convert to string?
        }
    }

    private void determineArrayType(String fieldName, ArrayNode array, SchemaBuilder builder) {
        if (!array.isEmpty()) {
            final JsonNodeType[] arrType = {null};
            final boolean[] forAll = {true};
            array.elements().forEachRemaining(node -> {
                JsonNodeType type = node.getNodeType();
                if (arrType[0] == null) {
                    arrType[0] = node.getNodeType();
                }
                if (type != arrType[0]) {
                    forAll[0] = false;
                }
            });

            if (arrType[0] == JsonNodeType.ARRAY) {

            } else if (arrType[0] == JsonNodeType.OBJECT) {

            } else {
                builder.field(fieldName, schemaFromPrimitive(arrType[0]));
            }
        }


    }

    private Schema schemaFromPrimitive(JsonNodeType node) {
        return null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    private R newNullRecord(R record) {
        return record.newRecord(record.topic(), record.kafkaPartition(), null, null, null, null, record.timestamp(), record.headers());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        JSON_LEVEL_VALUE = config.getInt(JSON_LEVEL);
        if (JSON_LEVEL_VALUE < 0){
            throw new ConfigException(String.format("%s must be >= 0",JSON_LEVEL));
        }

    }
}
