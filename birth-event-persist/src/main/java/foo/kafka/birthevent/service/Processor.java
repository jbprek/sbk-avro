package foo.kafka.birthevent.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthMapper;
import foo.kafka.birthevent.eventstore.persistence.BirthRepository;
import foo.kafka.common.MessageCoordinates;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class Processor {

    private final BirthMapper mapper;
    private final BirthRepository repository;

    @Transactional
    public void process(Message<BirthEvent> message) {
        BirthEvent event = message.getPayload();
        String key = message.getHeaders().get(KafkaHeaders.RECEIVED_KEY, String.class);
        String coordinates = MessageCoordinates.of(message).toString();
        Acknowledgment ack = message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        if (ack == null) {
            log.warn("No acknowledgment found in message headers at {}", coordinates);
            return;
        }

        Birth entity = mapper.toEntity(message.getPayload());
        try {
            Birth saved = repository.save(entity);
            ack.acknowledge();
            log.info("Persisted BirthEvent: {}", saved);
            log.info("Consumed BirthEvent at {}: with key={},value={}", coordinates, key, event);
        } catch (Exception e) {
            log.warn("Error persisting BirthEvent at {} : {} ", coordinates, e.getMessage());
            // pause briefly before retrying
            ack.nack(Duration.ofMillis(100L));
        }

    }
}
