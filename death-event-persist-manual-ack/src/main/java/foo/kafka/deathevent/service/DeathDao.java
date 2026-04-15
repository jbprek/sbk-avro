package foo.kafka.deathevent.service;

import foo.avro.death.DeathEvent;
import foo.kafka.deathevent.eventstore.persistence.FemaleDeathMapper;
import foo.kafka.deathevent.eventstore.persistence.FemaleDeathRepository;
import foo.kafka.deathevent.eventstore.persistence.MaleDeathMapper;
import foo.kafka.deathevent.eventstore.persistence.MaleDeathRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeathDao implements EventDao<DeathEvent> {

    private final MaleDeathMapper maleDeathMapper;
    private final FemaleDeathMapper femaleDeathMapper;
    private final MaleDeathRepository maleDeathRepository;
    private final FemaleDeathRepository femaleDeathRepository;

    @Override
    public void save(DeathEvent event) {
        String gender = event.getGender();
        if ("M".equals(gender)) {
            log.debug("Routing DeathEvent id={} to male_deaths", event.getId());
            maleDeathRepository.saveAndFlush(maleDeathMapper.toEntity(event));
        } else if ("F".equals(gender)) {
            log.debug("Routing DeathEvent id={} to female_deaths", event.getId());
            femaleDeathRepository.saveAndFlush(femaleDeathMapper.toEntity(event));
        } else {
            throw new IllegalArgumentException(
                    "Unknown or null gender '%s' for DeathEvent id=%s — skipping".formatted(gender, event.getId()));
        }
    }
}

