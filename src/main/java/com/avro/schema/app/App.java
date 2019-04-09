package com.avro.schema.app;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Hello world!
 *
 */
public class App
{
    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    public static final String DEFAULT_SCHEMA_REG_URL = "http://localhost:9090/api/v1";
    private final Map<String, Object> config;
    private final SchemaRegistryClient schemaRegistryClient;


    public static Map<String, Object> createConfig(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10L);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000L);
        return config;
    }

    public App() {
        this(createConfig(DEFAULT_SCHEMA_REG_URL));
    }

    public App(Map<String, Object> config) {
        this.config = config;
        schemaRegistryClient = new SchemaRegistryClient(config);
    }

    public void runAvroSerDesApis() throws IOException {
        //using builtin avro serializer/deserializer
        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        Object deviceObject = createGenericRecordForDevice("/device.avsc");

        SchemaMetadata schemaMetadata = createSchemaMetadata("avro-serializer-schema-" + System.currentTimeMillis());
        byte[] serializedData = avroSnapshotSerializer.serialize(deviceObject, schemaMetadata);
        Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), null);

        LOG.info("Serialized and deserialized objects are equal: [{}] ", deviceObject.equals(deserializedObj));
    }

    protected Object createGenericRecordForDevice(String schemaFileName) throws IOException {
        Schema schema = new Schema.Parser().parse(getSchema(schemaFileName));

        GenericRecord avroRecord = new GenericData.Record(schema);
        long now = System.currentTimeMillis();
        avroRecord.put("xid", now);
        avroRecord.put("name", "foo-" + now);
        avroRecord.put("version", new Random().nextInt());
        avroRecord.put("timestamp", now);

        return avroRecord;
    }

    public static void main(String[] args)throws Exception {

        String schemaRegistryUrl = System.getProperty(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), DEFAULT_SCHEMA_REG_URL);
        Map<String, Object> config = createConfig(schemaRegistryUrl);

        App app = new App(config);
        app.runSchemaApis();
        app.runAvroSerDesApis();
    }

    private SchemaMetadata createSchemaMetadata(String name) {
        return new SchemaMetadata.Builder(name)
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup("sample-group")
                .description("Sample schema")
                .compatibility(SchemaCompatibility.BACKWARD)
                .build();
    }

    public void runSchemaApis() throws Exception {

        String schemaFileName = "/device.avsc";
        String schema1 = getSchema(schemaFileName);
        SchemaMetadata schemaMetadata = createSchemaMetadata("com.hwx.schemas.samples-" + System.currentTimeMillis());

        // registering a new schema
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "Initial version of the schema"));
        LOG.info("Registered schema [{}] and returned version [{}]", schema1, v1);

    }

    private String getSchema(String schemaFileName) throws IOException {
        InputStream schemaResourceStream = App.class.getResourceAsStream(schemaFileName);
        if (schemaResourceStream == null) {
            throw new IllegalArgumentException("Given schema file [" + schemaFileName + "] does not exist");
        }

        return IOUtils.toString(schemaResourceStream, "UTF-8");
    }

}
