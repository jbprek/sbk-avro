package foo.kafka.processor.service;

import foo.avro.birth.BirthEvent;
import foo.avro.birth.BirthStatEntry;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class Processor {
    private final EntityManager entityManager;
    private final EventMapper eventMapper;


    public BirthStatEntry process(Message<BirthEvent> event) {
        var meta = MessageCoordinates.of(event);
        log.debug("[TX] Starting Processing event  at: {} : {})", meta, event);
        return eventMapper.eventToEntity(event.getPayload());
    }
}
