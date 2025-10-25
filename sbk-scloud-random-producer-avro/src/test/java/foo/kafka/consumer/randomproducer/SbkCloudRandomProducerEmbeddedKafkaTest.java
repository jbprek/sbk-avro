package foo.kafka.consumer.randomproducer;

import foo.avro.birth.BirthEvent;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"birth.register.avro"})
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class SbkCloudRandomProducerEmbeddedKafkaTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void produceAndConsumeAvroBirthEvent() throws Exception {
        // Build a simple BirthEvent (only set id and name, other fields can be null)
        BirthEvent event = BirthEvent.newBuilder()
                .setId(123L)
                .setName("Alice")
                .build();

        // Serialize to Avro binary
        byte[] bytes;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DatumWriter<BirthEvent> writer = new SpecificDatumWriter<>(BirthEvent.class);
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(event, encoder);
            encoder.flush();
            bytes = out.toByteArray();
        }

        // Create producer using embedded broker
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, byte[]>(producerProps);

        producer.send(new ProducerRecord<>("birth.register.avro", "k1", bytes)).get();
        producer.close();

        // Create consumer and read the record
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(UUID.randomUUID().toString(), "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        Consumer<String, byte[]> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton("birth.register.avro"));

        ConsumerRecords<String, byte[]> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
        assertThat(records.count()).isGreaterThan(0);

        byte[] received = records.iterator().next().value();

        // Deserialize
        DatumReader<BirthEvent> reader = new SpecificDatumReader<>(BirthEvent.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(received, null);
        BirthEvent read = reader.read(null, decoder);

        assertThat(read).isNotNull();
        // id and name should match
        assertThat(read.getId()).isEqualTo(event.getId());
        assertThat(read.getName()).isEqualTo(event.getName());

        consumer.close();
    }
}
