package foo.kafka.processor;

import foo.avro.birth.BirthEvent;
import foo.avro.birth.BirthStatEntry;
import foo.avro.birth.Gender;
import foo.kafka.processor.service.EventMapper;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest(classes = BirthEventProcessorApplication.class, properties = {
        "logging.level.foo.kafka.processor=DEBUG"
})
@EmbeddedKafka(partitions = 1, topics = {
        "birth.register.avro.mock",
        "birth.stats.avro.mock"
})
@DirtiesContext
class BirthEventProcessorApplicationTest {

    @Autowired
    private EmbeddedKafkaBroker broker;
    @Autowired
    private EventMapper mapper;

    Consumer<String, BirthStatEntry> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = KafkaTestUtils.consumerProps("mock-group", "false", broker);
        configs.put(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, "100");
        configs.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1");
        configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        configs.put("schema.registry.url", "mock://embedded-registry");
        DefaultKafkaConsumerFactory<String, BirthStatEntry> cf =
                new DefaultKafkaConsumerFactory<>(configs);
        consumer = cf.createConsumer();
        consumer.commitSync();
        // consume from the output topic to read the processed BirthStatEntry
        broker.consumeFromEmbeddedTopics(consumer, "birth.stats.avro.mock");
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    @DisplayName("An incoming BirthEvent is processed and sent to the output topic as BirthStatEntry")
    void testBirthEventProcessing() {

        var birthEventIn = BirthEvent.newBuilder()
                .setId(1L)
                .setName("John")
                .setDob(LocalDate.EPOCH)
                .setTown("Athens")
                .setRegistrationTime(Instant.EPOCH)
                .setWeight(new BigDecimal("3.5"))
                .setGender(Gender.MALE)
                .build();

        Map<String, Object> producerConfigs = KafkaTestUtils.producerProps(broker);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfigs.put("schema.registry.url", "mock://embedded-registry");
        producerConfigs.put("auto.register.schemas", "true");
        producerConfigs.put("specific.avro.writer", "true");
        KafkaTemplate<String, BirthEvent> producer = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(producerConfigs));
        producer.send("birth.register.avro.mock", birthEventIn.getTown(), birthEventIn).join();
        producer.flush();

        ConsumerRecords<String, BirthStatEntry> kakfaRecords = consumer.poll(Duration.ofMillis(2000));
        var kafakRecord = kakfaRecords.iterator().next();
        var birthStatOut = kafakRecord.value();
        var expectedStatEntry = mapper.eventToStatEntry(birthEventIn);
        assertThat(birthStatOut)
                .as("Processed BirthStatEntry should not be null")
                .isNotNull();
        assertThat(birthStatOut.getId()).as("ids should match").isEqualTo(expectedStatEntry.getId());
        assertThat(birthStatOut.getDob()).as("dob should match").isEqualTo(expectedStatEntry.getDob());
        assertThat(birthStatOut.getTown()).as("town should match").isEqualTo(expectedStatEntry.getTown());
    }
}