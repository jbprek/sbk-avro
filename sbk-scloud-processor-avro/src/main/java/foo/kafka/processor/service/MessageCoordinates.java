package foo.kafka.processor.service;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

public record MessageCoordinates(String topic, Integer partition, Long offset){

    public static MessageCoordinates of(Message<?> message) {
        return new MessageCoordinates(
                (String) message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC),
                (Integer) message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION),
                (Long) message.getHeaders().get(KafkaHeaders.OFFSET));
    }

    @Override
    public String toString() {
        return "message %s(%d,%d)".formatted(topic, partition, offset);
    }
}
