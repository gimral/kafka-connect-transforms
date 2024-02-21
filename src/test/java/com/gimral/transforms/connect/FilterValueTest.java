package com.gimral.transforms.connect;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FilterValueTest {
    private static final String topicName = "test";
    private static final String SCHEMA_REGISTRY_URL_CONFIG = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
    private static final String schemaRegistryUrl = "mock://";
    private final Filter<SourceRecord> valueFilter = new Filter.Value<>();
    private SourceRecord jsonSourceRecord;
    private SourceRecord invalidJsonSourceRecord;
    private SourceRecord avroSourceRecord;
    private SourceRecord invalidAvroSourceRecord;

    @BeforeAll
    public void setupAll(){
        jsonSourceRecord = prepareJsonByteSourceRecord();
        invalidJsonSourceRecord = prepareInvalidJsonByteSourceRecord();
        avroSourceRecord = prepareSchemaRegistryAvroSourceRecord();
        invalidAvroSourceRecord = prepareInvalidJsonByteSourceRecord();
    }

    @AfterEach
    public void teardown() {
        valueFilter.close();
    }

    @Test
    public void jsonRecordShouldBeFilteredForMatchingValueCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.name == 'A')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(jsonSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(jsonSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void jsonRecordShouldNotBeFilteredForNonMatchingValueCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.name == 'B')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(jsonSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void jsonRecordShouldBeFilteredForMatchingValueNestedCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.address.city == 'Dubai')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(jsonSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(jsonSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void jsonRecordShouldNotBeFilteredForMatchingValueConditionWhenNegated() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("negate", true);
        props.put("condition", "$[?(@.name == 'A')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(jsonSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void jsonRecordShouldBeFilteredForNonMatchingValueConditionWhenNegated() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("negate", true);
        props.put("condition", "$[?(@.name == 'B')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(jsonSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(jsonSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void invalidJsonRecordShouldBeFilteredWhenOnErrorIsDefault() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.name == 'A')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(invalidJsonSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void invalidJsonRecordShouldBeFilteredWhenOnErrorIsExclude() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.name == 'A')]");
        props.put("on.error", "exclude");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(invalidJsonSourceRecord);

        assertNull(transformedRecord, "Record not should be filtered");
    }

    @Test
    public void invalidJsonRecordShouldBeFilteredWhenOnErrorIsInclude() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.name == 'A')]");
        props.put("on.error", "include");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(invalidJsonSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(invalidJsonSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void invalidJsonRecordShouldThrowExceptionWhenOnErrorIsError() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "json");
        props.put("condition", "$[?(@.name == 'A')]");
        props.put("on.error", "error");
        valueFilter.configure(props);

        assertThrows(DataException.class,() -> valueFilter.apply(invalidJsonSourceRecord));
    }

    @Test
    public void avroRecordShouldBeFilteredForMatchingValueCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.name == 'A')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(avroSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(avroSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void avroRecordShouldNotBeFilteredForNonMatchingValueCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.name == 'B')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(avroSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void avroRecordShouldBeFilteredForMatchingValueNestedCondition() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.address.city == 'Dubai')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(avroSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(avroSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void avroRecordShouldNotBeFilteredForMatchingValueConditionWhenNegated() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("negate", true);
        props.put("condition", "$[?(@.name == 'A')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(avroSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void avroRecordShouldBeFilteredForNonMatchingValueConditionWhenNegated() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.name == 'B')]");
        props.put("negate", true);
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(avroSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(avroSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void invalidAvroRecordShouldBeFilteredWhenOnErrorIsDefault() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.name == 'A')]");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(invalidAvroSourceRecord);

        assertNull(transformedRecord, "Record should be filtered");
    }

    @Test
    public void invalidAvroRecordShouldBeFilteredWhenOnErrorIsExclude() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.name == 'A')]");
        props.put("on.error", "exclude");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(invalidAvroSourceRecord);

        assertNull(transformedRecord, "Record should not be filtered");
    }

    @Test
    public void invalidAvroRecordShouldBeFilteredWhenOnErrorIsInclude() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.name == 'A')]");
        props.put("on.error", "include");
        valueFilter.configure(props);

        final SourceRecord transformedRecord = valueFilter.apply(invalidAvroSourceRecord);

        assertNotNull(transformedRecord, "Record should be filtered");
        assertEquals(invalidAvroSourceRecord,transformedRecord, "Record should be same");
    }

    @Test
    public void invalidAvroRecordShouldThrowExceptionWhenOnErrorIsError() {
        final Map<String, Object> props = new HashMap<>();
        props.put("format", "avro");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put("condition", "$[?(@.name == 'A')]");
        props.put("on.error", "error");
        valueFilter.configure(props);

        assertThrows(DataException.class,() -> valueFilter.apply(invalidAvroSourceRecord));
    }

    private SourceRecord prepareJsonByteSourceRecord(){
        var keyJson =  "{\"id\":\"1\"}";
        var valueJson =  "{\"name\": \"A\", \"age\":10, \"address\": {\"city\":\"Dubai\",\"country\":\"UAE\"}}";
        return prepareSourceRecord(keyJson.getBytes(), valueJson.getBytes());
    }

    private SourceRecord prepareInvalidJsonByteSourceRecord(){
        var keyJson =  "\"id\":\"1\"}";
        var valueJson =  "{\"name\": \"A\", \"age\"10, \"address\": \"city\":\"Dubai\",\"country\":\"UAE\"}}";
        return prepareSourceRecord(keyJson.getBytes(), valueJson.getBytes());
    }

    private SourceRecord prepareSourceRecord(Object key, Object value){
       return  new SourceRecord(null, null, topicName, 0,
                null, key,null, value);
    }

    private SourceRecord prepareSchemaRegistryAvroSourceRecord(){
        Schema.Parser parser = new org.apache.avro.Schema.Parser();
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
