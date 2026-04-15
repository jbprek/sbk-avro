package foo.kafka.common;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

public record MessageCoordinates(String topic, Integer partition, Long offset, Instant timestamp) {

    public static MessageCoordinates of(RecordMetadata md) {
        return Optional.ofNullable(md)
                .map(meta -> new MessageCoordinates(meta.topic(), meta.partition(), meta.offset(), Instant.ofEpochMilli(meta.timestamp())))
                .orElse(null);
    }

    public static MessageCoordinates of(Message<?> message) {
        return Optional.ofNullable(message).map(Message::getHeaders)
                .map(headers -> new MessageCoordinates(
                        (String) headers.get(KafkaHeaders.RECEIVED_TOPIC),
                        (Integer) headers.get(KafkaHeaders.RECEIVED_PARTITION),
                        (Long) headers.get(KafkaHeaders.OFFSET),
                        Instant.ofEpochMilli((Long) headers.get(KafkaHeaders.RECEIVED_TIMESTAMP))))
                .orElse(null);
    }

    @Override
    public String toString() {
        return String.format("%s(%s,%s,%s)",
                Objects.toString(topic(), "null"),
                Objects.toString(partition(), "-1"),
                Objects.toString(offset(), "-1"),
                Objects.toString(timestamp(), "-1"));
    }
}
