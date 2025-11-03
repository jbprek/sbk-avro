package foo.kafka.birthevent.service;

import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class BirthDao {

    private final BirthRepository repository;

    public void saveBirthEvent(Birth entity) {
        repository.saveAndFlush(entity);
    }
}
