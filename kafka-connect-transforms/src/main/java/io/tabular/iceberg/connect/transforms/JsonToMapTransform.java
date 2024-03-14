package io.tabular.iceberg.connect.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.*;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.List;
import java.util.Map.Entry;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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


    private static final Schema ALL_JSON_SCHEMA = SchemaBuilder.struct().field("value", Schema.STRING_SCHEMA).build();

    public static final Map<Class<? extends JsonNode>, Schema> JSON_NODE_TO_SCHEMA = getJsonNodeToSchema();

    private static Map<Class<? extends JsonNode>, Schema> getJsonNodeToSchema() {
        final Map<Class<? extends JsonNode>, Schema> map = Maps.newHashMap();
        map.put(BinaryNode.class, Schema.OPTIONAL_BYTES_SCHEMA);
        map.put(BooleanNode.class, Schema.OPTIONAL_BOOLEAN_SCHEMA);
        map.put(TextNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        map.put(IntNode.class, Schema.OPTIONAL_INT32_SCHEMA);
        map.put(LongNode.class, Schema.OPTIONAL_INT64_SCHEMA);
        map.put(FloatNode.class, Schema.OPTIONAL_FLOAT32_SCHEMA);
        map.put(DoubleNode.class, Schema.OPTIONAL_FLOAT64_SCHEMA);
        map.put(ArrayNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        map.put(ObjectNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        map.put(BigIntegerNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        map.put(DecimalNode.class, Schema.OPTIONAL_STRING_SCHEMA);
        return ImmutableMap.copyOf(map);
    }

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
        } catch (Exception e) {
            throw new RuntimeException(String.format("record.value is not valid json for record.value: %s", collectRecordDetails(record)), e);
        }

        if (!(obj instanceof ObjectNode)) {
            throw new RuntimeException(String.format("Expected Json Object for record.value: %s", collectRecordDetails(record)));
        }

        if (JSON_LEVEL_VALUE == 0) {
            // return the json as a single field, after validating it's actually json
            return singleField(record);
        }

        return record;
    }

    private R singleField(R record) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), ALL_JSON_SCHEMA, record.value(), record.timestamp(), record.headers());
    }

    private R structRecord(R record, ObjectNode contents) {
        SchemaBuilder builder = SchemaBuilder.struct();
        contents.fields().forEachRemaining(entry -> addFieldSchemaBuilder(entry, builder));
        Schema schema = builder.build();
        Struct value = addToStruct(contents, schema, new Struct(schema));
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), schema, value, record.timestamp(), record.headers());
    }

    private String collectRecordDetails(R record) {
        if (record instanceof SinkRecord) {
            SinkRecord sinkRecord = (SinkRecord) record;
            return            String.format("topic %s partition %s offset %s", sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
        } else {
            return String.format("topic %s partition %S", record.topic(), record.kafkaPartition());
        }
    }

    private void addFieldSchemaBuilder(Entry<String, JsonNode> kv, SchemaBuilder builder) {
        String key = kv.getKey();
        JsonNode value = kv.getValue();
        if (!value.isNull() || !value.isMissingNode()) {
            if (value.isArray()) {
                // array time, need to confirm all records are of the same type.
                ArrayNode array = (ArrayNode) value;
                if (!array.isEmpty()) {
                    Class<? extends JsonNode> arrayType = determineArrayNodeType(array);
                    if (arrayType != null) {
                        if (JsonNodeType.NULL.equals(arrayType) || JsonNodeType.MISSING.equals(arrayType)) {
                            builder.field(key, SchemaBuilder.array(JSON_NODE_TO_SCHEMA.get(arrayType)).optional().build());
                        } else {
                            builder.field(key, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
                        }
                    }
                }
            } else {
                builder.field(key, JSON_NODE_TO_SCHEMA.get(value.getClass()));
            }
        }
    }

    // handle the empty object cast?
    private Class<? extends JsonNode> determineArrayNodeType(ArrayNode array) {
        final List<Class<? extends JsonNode>> arrayType = Lists.newArrayList();
        arrayType.add(null);
        final boolean[] allTypesConsistent = {true};
        // breaks on number.
        array.elements().forEachRemaining(node -> {
            Class<? extends JsonNode> type = node.getClass();
            if (arrayType.get(0) == null) {
                arrayType.set(0,type);
            }
            if (type != arrayType.get(0)) {
                allTypesConsistent[0] = false;
            }
        });

        if (!allTypesConsistent[0]) {
            return null;
        }
        return arrayType.get(0);
    }

    private Struct addToStruct(ObjectNode node, Schema schema, Struct struct) {
        schema.fields().forEach(field -> {
            JsonNode element = node.get(field.name());
            // adding primitives other than the weird array of primitive case
            Schema.Type targetType = field.schema().type();
            if (Objects.requireNonNull(targetType) == Schema.Type.ARRAY) {
                Schema.Type arrayType = field.schema().valueSchema().type();
                List<Object> elements = Lists.newArrayList();
                element.elements().forEachRemaining(arrayEntry -> elements.add(primitiveBasedOnSchema(arrayEntry, arrayType, field.name())));
                struct.put(field.name(), elements);
            } else {
                struct.put(field.name(), primitiveBasedOnSchema(element, targetType, field.name()));
            }
        });
        return struct;
    }

    private Object primitiveBasedOnSchema(JsonNode node, Schema.Type type, String fieldName) {
        Object obj;
        switch (type) {
            case STRING:
                obj = node.toString();
                break;
            case BOOLEAN:
                obj = node.booleanValue();
                break;
            case INT32:
                obj = node.intValue();
                break;
            case INT64:
                obj = node.longValue();
                break;
            case FLOAT32:
                obj = node.floatValue();
                break;
            case FLOAT64:
                obj = node.doubleValue();
                break;
            case BYTES:
                // not sure here if you dig out
                try {
                    obj = node.binaryValue();
                } catch (Exception e) {
                    throw new RuntimeException(String.format("parsing binary value threw exception for %s", fieldName), e);
                }
                break;
            default:
                throw new RuntimeException(String.format("Unexpected type %s for field %s", type, fieldName));
        }
        return obj;
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
