package ru.netology.dsw.dsl;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Простой Avro Serde для GenericRecord.
 * Использует Avro-схему, которая передаётся в конструктор.
 * Зависит только от org.apache.avro:avro (Maven Central).
 */
public class AvroSerde implements Serde<GenericRecord> {

    private final org.apache.avro.Schema schema;

    public AvroSerde(org.apache.avro.Schema schema) {
        this.schema = schema;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<GenericRecord> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                writer.write(data, encoder);
                encoder.flush();
                return out.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Avro serialization failed", e);
            }
        };
    }

    @Override
    public Deserializer<GenericRecord> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                return reader.read(null, decoder);
            } catch (IOException e) {
                throw new RuntimeException("Avro deserialization failed", e);
            }
        };
    }
}