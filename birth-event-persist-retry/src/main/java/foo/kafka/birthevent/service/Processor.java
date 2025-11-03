package foo.kafka.birthevent.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthMapper;
import foo.kafka.common.MessageCoordinates;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Processor {

    private final BirthMapper mapper;
    private final BirthDao dao;
    private final RetryTemplate retryTemplate;

    @Autowired
    public Processor(@Qualifier("birthMapperImpl") BirthMapper mapper, BirthDao dao, RetryTemplate retryTemplate) {
        this.mapper = mapper;
        this.dao = dao;
        this.retryTemplate = retryTemplate;
    }


    public void process(Message<BirthEvent> message) {
        BirthEvent event = message.getPayload();
        String key = message.getHeaders().get(KafkaHeaders.RECEIVED_KEY, String.class);
        String coordinates = MessageCoordinates.of(message).toString();

        Birth entity = mapper.toEntity(message.getPayload());
        retryTemplate.execute(
                context -> {
                    // Attempt to persist; if a non-transient exception occurs it will go to recovery immediately
                    dao.saveBirthEvent(entity);
                    log.info("Persisted BirthEvent: {}", entity);
                    log.info("Consumed BirthEvent at {}: with key={},value={}", coordinates, key, event);
                    return null;
                },
                // Recovery callback: retries exhausted or non-retryable exception
                context -> {
                    Throwable last = context.getLastThrowable();
                    log.error("Failed to persist BirthEvent at {} after retries; skipping. lastException={}", coordinates, last == null ? "<none>" : last.getMessage(), last);
                    // Acknowledge to skip this message after retries exhausted
                    return null;
                }
        );


    }


}
