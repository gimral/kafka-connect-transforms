package com.gimral.transforms.connect;

import com.gimral.transforms.connect.util.JsonConverter;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import com.gimral.transforms.connect.util.Requirements;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.io.ByteArrayInputStream;
import java.util.Map;

/**
 * Includes or filters all records from subsequent transformations in the chain.
 * This is intended to be used conditionally to filter out records matching (or not matching)
 * a particular JSONPath Predicate
 * @param <R> The type of record.
 */
public abstract class Filter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String CONDITION_CONFIG = "condition";
    private static final String NEGATE_CONFIG = "negate";
    private static final String FORMAT_CONFIG = "format";
    private static final String ON_ERROR_CONFIG = "on.error";
    private static final String SCHEMA_REGISTRY_URL_CONFIG = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

    private interface Format {
        String JSON = "json";
        String AVRO = "avro";
    }

    private interface OnError {
        String EXCLUDE = "exclude";
        String INCLUDE = "include";
        String ERROR = "error";
    }

    public static final String OVERVIEW_DOC = "Includes or filters all records from subsequent transformations in the chain. " +
            "This is intended to be used conditionally to filter out records matching (or not matching) " +
            "a particular JSONPath Predicate.";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONDITION_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "A JSONPath predicate for including or filtering records")
            .define(FORMAT_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.ValidString.in(Format.AVRO, Format.JSON), ConfigDef.Importance.HIGH,
                    "Format of the record")
            .define(SCHEMA_REGISTRY_URL_CONFIG, ConfigDef.Type.STRING, "",
                     ConfigDef.Importance.MEDIUM,
                    "Schema Registry Url to use for avro records")
            .define(NEGATE_CONFIG, ConfigDef.Type.BOOLEAN, false,
                    new ConfigDef.NonNullValidator(), ConfigDef.Importance.MEDIUM,
                    "Either filter or include records")
            .define(ON_ERROR_CONFIG, ConfigDef.Type.STRING, "exclude",
                    ConfigDef.ValidString.in(OnError.INCLUDE,OnError.EXCLUDE,OnError.ERROR), ConfigDef.Importance.MEDIUM,
                    "How to handle the records on error");

    private static final String PURPOSE = "Filter Records";

    private String format;
    private String condition;
    private boolean negate;
    private String onError;
    private KafkaAvroDeserializer avroDeserializer;
    private JsonConverter jsonConverter;
    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        format = config.getString(FORMAT_CONFIG);
        if (Format.AVRO.equalsIgnoreCase(format)) {
            avroDeserializer = new KafkaAvroDeserializer();
            avroDeserializer.configure(props, false);
            jsonConverter = new JsonConverter();
        }
        condition = config.getString(CONDITION_CONFIG);
        negate = config.getBoolean(NEGATE_CONFIG);
        onError = config.getString(ON_ERROR_CONFIG);
    }

    @Override
    public R apply(R record) {
        try {
            if (Format.AVRO.equalsIgnoreCase(format) &&
                    (applyAvroFilter(record) ^ negate))
                return record;
            if (Format.JSON.equalsIgnoreCase(format) &&
                    (applyJsonFilter(record) ^ negate))
                return record;
        }
        catch (Exception ex){
            if(OnError.EXCLUDE.equalsIgnoreCase(onError))
                return null;
            if(OnError.INCLUDE.equalsIgnoreCase(onError))
                return record;
            if(OnError.ERROR.equalsIgnoreCase(onError))
                throw ex;
        }
        return null;
    }

    private boolean applyAvroFilter(R record) {
        byte[] value = Requirements.requireByteArray(operatingValue(record), PURPOSE);
        try {
            GenericRecord genericRecord = (GenericRecord) avroDeserializer.deserialize("", value);
            byte[] jsonBytesValue = jsonConverter.fromAvro(genericRecord);

            return !JsonPath.parse(new ByteArrayInputStream(jsonBytesValue))
                    .read(condition, JSONArray.class)
                    .isEmpty();
        } catch (Exception e) {
            throw new DataException("Error applying Avro filter transformation", e);
        }
    }

    private boolean applyJsonFilter(R record) {
        byte[] value = Requirements.requireJson(operatingValue(record), PURPOSE);
        try {
            return !JsonPath.parse(new ByteArrayInputStream(value))
                    .read(condition, JSONArray.class)
                    .isEmpty();
        } catch (Exception e) {
            throw new DataException("Error applying Json filter transformation", e);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    protected abstract Object operatingValue(R record);

    public static class Key<R extends ConnectRecord<R>> extends Filter<R> {

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends Filter<R> {

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

    }

}
