package foo.kafka.birthevent.service;

import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class BirthDao implements EventDao<Birth> {

    private final BirthRepository repository;

    @Override
    public void save(Birth entity) {
        repository.saveAndFlush(entity);
    }
}
