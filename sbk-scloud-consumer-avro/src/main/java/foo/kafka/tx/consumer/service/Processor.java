package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
@RequiredArgsConstructor
@Service
public class Processor {
    private final EntityManager entityManager;
    private final EventMapper eventMapper;


    /**
     * Binding name: 'process' -> maps to 'process-in-0' in application.yml
     * Cloud Stream Kafka binder starts a Kafka TX on the consumer thread.
     * This method starts a JPA TX; DB commit must succeed before offset commit.
     */
    @Transactional
    public void process(Message<BirthEvent> event) {
        var meta = MessageInfo.of(event);
        log.debug("[TX] Starting transaction for event at: {} : {})", meta, event);
        var entity = eventMapper.eventToEntity(event.getPayload());
        entityManager.persist(entity);

        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                switch (status) {
                    case STATUS_COMMITTED ->
                            log.info("[TX] Transaction committed for event at:{} {} stored as {} )", meta, event, entity);
                    case STATUS_ROLLED_BACK ->
                            log.error("[TX] Transaction rolled back for for event at:{} {} ", meta, event);
                    default ->
                            log.warn("[TX] Transaction status unknown for for event at:{} {} ", meta, event);
                }
            }
        });
    }
}
