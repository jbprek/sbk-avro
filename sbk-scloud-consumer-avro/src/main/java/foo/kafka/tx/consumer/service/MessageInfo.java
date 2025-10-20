package foo.kafka.tx.consumer.service;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

public record  MessageInfo(String topic, Integer partition, Long offset){

    public static MessageInfo of(Message<?> message) {
        return new MessageInfo(
                (String) message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC),
                (Integer) message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION),
                (Long) message.getHeaders().get(KafkaHeaders.OFFSET));
    }

    @Override
    public String toString() {
        return "topic='%s', partition=%d, offset=%d".formatted(topic, partition, offset);
    }
}
