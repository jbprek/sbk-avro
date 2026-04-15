package foo.kafka.deathevent.eventstore.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MaleDeathRepository extends JpaRepository<MaleDeath, Long> {
}

