package foo.kafka.common;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.util.Objects;
import java.util.Optional;

public record MessageCoordinates(String topic, Integer partition, Long offset) {

    public static MessageCoordinates of(RecordMetadata md) {
        return Optional.ofNullable(md)
                .map(meta -> new MessageCoordinates(meta.topic(), meta.partition(), meta.offset()))
                .orElse(null);
    }

    public static MessageCoordinates of(Message<?> message) {
        return Optional.ofNullable(message).map(Message::getHeaders)
                .map(headers -> new MessageCoordinates(
                        (String) headers.get(KafkaHeaders.RECEIVED_TOPIC),
                        (Integer) headers.get(KafkaHeaders.RECEIVED_PARTITION),
                        (Long) headers.get(KafkaHeaders.OFFSET)))
                .orElse(null);
    }

    @Override
    public String toString() {
        return "%s(%s,%s)".formatted(
                Objects.toString(topic(), "null"),
                Objects.toString(partition(), "-1"),
                Objects.toString(offset(), "-1"));
    }
}
