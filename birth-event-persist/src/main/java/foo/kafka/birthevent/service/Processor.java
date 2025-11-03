package foo.kafka.birthevent.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthMapper;
import foo.kafka.common.MessageCoordinates;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

import java.sql.SQLTransientException;
import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class Processor {

    private final BirthMapper mapper;
    private final BirthDao dao;


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
        Exception lastException = null;

        try {
            dao.saveBirthEvent(entity);
            ackMessage(ack, coordinates);
            log.info("Persisted BirthEvent: {}", entity);
            log.info("Consumed BirthEvent at {}: with key={},value={}", coordinates, key, event);
            return; // Success
        } catch (Exception e) {
            if (isTransient(e)) {
                nackMessage(ack, Duration.ofMillis(500), coordinates);
                log.warn("Transient Error persisting BirthEvent at {} : {} ",
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
        if (t instanceof TransientDataAccessException) {
            return true;
        }
        // JDBC's SQLTransientException and subclasses
        if (t instanceof SQLTransientException) {
            return true;
        }
        // Some drivers wrap transient SQL exceptions in generic SQLExceptions or other wrappers.
        // Inspect class name for common 'Transient' indicator as a fallback.
        if (t instanceof java.sql.SQLException) {
            String cls = t.getClass().getSimpleName();
            if (cls != null && cls.toLowerCase().contains("transient")) {
                return true;
            }
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
