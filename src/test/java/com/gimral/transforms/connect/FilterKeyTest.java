package com.gimral.transforms.connect;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilterKeyTest {
    private static final String topicName = "test";
    private static final String SCHEMA_REGISTRY_URL_CONFIG = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
    private static final String schemaRegistryUrl = "mock://";
    private final Filter<SourceRecord> keyFilter = new Filter.Key<>();
    private SourceRecord jsonSourceRecord;
    private SourceRecord avroSourceRecord;

    @BeforeAll
    public void setupAll(){
        jsonSourceRecord = prepareJsonByteSourceRecord();
        avroSourceRecord = prepareSchemaRegistryAvroSourceRecord();
    }

    @AfterEach
    public void teardown() {
        keyFilter.close();
    }

    @Test
    public void jsonRecordShouldBeFilteredForMatchingValueCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.id == '1')]");
        keyFilter.configure(props);

        final SourceRecord transformedRecord = keyFilter.apply(jsonSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(jsonSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void jsonRecordShouldNotBeFilteredForNonMatchingValueCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.id == '2')]");
        keyFilter.configure(props);

        final SourceRecord transformedRecord = keyFilter.apply(jsonSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void jsonRecordShouldNotBeFilteredForMatchingValueConditionWhenNegated() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("negate", true);
        props.put("condition", "$[?(@.id == '1')]");
        keyFilter.configure(props);

        final SourceRecord transformedRecord = keyFilter.apply(jsonSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void jsonRecordShouldBeFilteredForNonMatchingValueConditionWhenNegated() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.name == '2')]");
        keyFilter.configure(props);

        final SourceRecord transformedRecord = keyFilter.apply(jsonSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void avroRecordShouldBeFilteredForMatchingValueCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.id == '1')]");
        keyFilter.configure(props);

        final SourceRecord transformedRecord = keyFilter.apply(avroSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(avroSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void avroRecordShouldNotBeFilteredForNonMatchingValueCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.id == '2')]");
        keyFilter.configure(props);

        final SourceRecord transformedRecord = keyFilter.apply(avroSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void avroRecordShouldNotBeFilteredForMatchingValueConditionWhenNegated() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("negate", true);
        props.put("condition", "$[?(@.id == '1')]");
        keyFilter.configure(props);

        final SourceRecord transformedRecord = keyFilter.apply(avroSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void avroRecordShouldBeFilteredForNonMatchingValueConditionWhenNegated() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("negate", true);
        props.put("condition", "$[?(@.id == '2')]");
        keyFilter.configure(props);

        final SourceRecord transformedRecord = keyFilter.apply(avroSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(avroSourceRecord,transformedRecord, "Record should be same");
    }

    private SourceRecord prepareJsonByteSourceRecord(){
        var keyJson =  "{\"id\":\"1\"}";
        var valueJson =  "{\"name\": \"A\", \"age\":10, \"address\": {\"city\":\"Dubai\",\"country\":\"UAE\"}}";
        return prepareSourceRecord(keyJson.getBytes(), valueJson.getBytes());
    }

    private SourceRecord prepareSourceRecord(Object key, Object value){
       return  new SourceRecord(null, null, topicName, 0,
                null, key,null, value);
    }

    private SourceRecord prepareSchemaRegistryAvroSourceRecord(){
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"MyRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"age\",\n" +
                "      \"type\": \"int\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"address\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"record\",\n" +
                "        \"name\": \"Address\",\n" +
                "        \"fields\": [\n" +
                "          {\n" +
                "            \"name\": \"city\",\n" +
                "            \"type\": \"string\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"country\",\n" +
                "            \"type\": \"string\"\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}");
        GenericRecord value = new GenericData.Record(schema);
        value.put("name", "A");
        value.put("age", 10);
        GenericRecord address = new GenericData.Record(schema.getField("address").schema());
        address.put("city", "Dubai");
        address.put("country", "UAE");

        // Set the nested address record
        value.put("address", address);

        Schema keySchema = parser.parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"IdRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");

        GenericRecord key = new GenericData.Record(keySchema);
        key.put("id", "1");

        try(KafkaAvroSerializer keySerializer = new KafkaAvroSerializer();
            KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer()) {
            Map<String, String> props = new HashMap<>();
            props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            keySerializer.configure(props, true);
            valueSerializer.configure(props, false);
            byte[] keyBytes = keySerializer.serialize(topicName, key);
            byte[] valueBytes = valueSerializer.serialize(topicName, value);
            return prepareSourceRecord(keyBytes, valueBytes);
        }
    }


}
