package foo.kafka.tx.producer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.producer.persistence.Birth;
import foo.kafka.tx.producer.persistence.BirthRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
@Service
public class ProcessorService {

    private final KafkaTemplate<String, BirthEvent> kafkaTemplate;
    private final BirthRepository repo;
    private final BirthMapper mapper;
    @Value("${spring.kafka.topic}")
    private String topic;


    /**
     * Single-method, one TransactionManager (DB/JPA).
     * Kafka producer TX is synchronized with this DB TX (DB commits first, then Kafka).
     */
    @Transactional(transactionManager = "dstm", propagation = Propagation.REQUIRES_NEW)
    public void sendAndStore(Birth birth) {
        log.info("[TX] start Processing birth: {}", birth);
        var exists = repo.existsById(birth.getId());
        if (exists) {
            log.warn("[TX] Skipping duplicate  {}", birth);
            return;
        }
        try {
            // Flushing forces Hibernate to execute the insert before we send to Kafka, so a unique-constraint violation surfaces
            var user = repo.saveAndFlush(birth);

            var event = mapper.toBirthEvent(birth);
            kafkaTemplate.send(topic, event.getId().toString(), event);

            return;
        } catch (DataIntegrityViolationException dive) {
            // Handle race: another thread inserted the same id between our check and insert.   Roll back DB TX (rethrow)
            log.warn("[TX] Unique constraint hit {}, treating as idempotent", birth, dive.getMessage());
            if (!repo.existsById(birth.getId())) {
                throw dive;
            }
        }
    }
}
