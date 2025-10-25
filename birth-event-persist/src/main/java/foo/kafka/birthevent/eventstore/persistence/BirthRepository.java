package foo.kafka.birthevent.eventstore.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BirthRepository extends JpaRepository<Birth, Long> {
}


