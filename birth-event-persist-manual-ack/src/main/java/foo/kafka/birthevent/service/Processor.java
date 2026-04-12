package foo.kafka.birthevent.service;

import foo.kafka.common.MessageCoordinates;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.sql.SQLTransientException;
import java.time.Duration;

/**
 * Generic processor that maps an incoming Kafka event of type {@code E}
 * to an entity of type {@code T}, persists it, and handles manual ack/nack.
 *
 * @param <E> the event (message payload) type
 * @param <T> the entity type
 */
@Slf4j
@RequiredArgsConstructor
public class Processor<E, T> {

    private final EventMapper<E, T> mapper;
    private final EventDao<T> dao;


    public void process(Message<E> message) {
        E event = message.getPayload();
        String key = message.getHeaders().get(KafkaHeaders.RECEIVED_KEY, String.class);
        String coordinates = MessageCoordinates.of(message).toString();
        Acknowledgment ack = message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        if (ack == null) {
            log.warn("No acknowledgment found in message headers at {}", coordinates);
            return;
        }

        T entity = mapper.toEntity(event);

        try {
            dao.save(entity);
            ackMessage(ack, coordinates);
            log.info("Persisted event: {}", entity);
            log.info("Consumed event at {}: with key={},value={}", coordinates, key, event);
        } catch (Exception e) {
            if (isTransient(e)) {
                nackMessage(ack, Duration.ofMillis(500), coordinates);
                log.warn("Transient Error persisting event at {} : {} ",
                        coordinates, e.getMessage(), e);

            } else {
                ackMessage(ack, coordinates);
                log.error("Not Transient Error persisting BirthEvent at {} : {}, skipping message ",
                        coordinates, e.getMessage());
            }
        }
    }


    private boolean isTransient(Throwable t) {
        while (t != null) {
            // Spring's TransientDataAccessException
            if (t instanceof TransientDataAccessException ||
                    t instanceof SQLTransientException) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private void ackMessage(Acknowledgment ack, String coordinates) {
        try {
            ack.acknowledge();
            log.debug("Acknowledged message at {}", coordinates);
        } catch (Exception ackEx) {
            log.warn("Failed to acknowledge message at {} : {}", coordinates, ackEx.getMessage());
            throw new RuntimeException("Failed to acknowledge message at %s".formatted(coordinates), ackEx);
        }
    }

    private void nackMessage(Acknowledgment ack, Duration duration, String coordinates) {
        try {
            ack.nack(duration);
            log.debug("Nack-ed message at {}", coordinates);
        } catch (Exception ackEx) {
            log.warn("Failed to Nack message at {} : {}", coordinates, ackEx.getMessage());
            throw new RuntimeException("Failed to Nack message at %s".formatted(coordinates), ackEx);
        }
    }
}
