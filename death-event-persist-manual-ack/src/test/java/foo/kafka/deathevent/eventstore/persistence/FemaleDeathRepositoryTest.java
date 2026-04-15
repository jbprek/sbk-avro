package foo.kafka.deathevent.eventstore.persistence;

import jakarta.persistence.PersistenceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
class FemaleDeathRepositoryTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private FemaleDeathRepository repository;

    private FemaleDeath entity;

    @BeforeEach
    void setUp() {
        entity = new FemaleDeath();
        entity.setId(2L);
        entity.setName("Jane Smith");
        entity.setDod(LocalDate.of(2025, 3, 20));
        entity.setTown("Manchester");
    }

    @Test
    @DisplayName("Saved FemaleDeath can be retrieved by id")
    void testSaveAndFindById() {
        entityManager.persistAndFlush(entity);
        entityManager.clear();

        var found = repository.findById(2L);

        assertThat(found).isPresent().hasValueSatisfying(e -> {
            assertThat(e.getId()).isEqualTo(2L);
            assertThat(e.getName()).isEqualTo("Jane Smith");
            assertThat(e.getDod()).isEqualTo(LocalDate.of(2025, 3, 20));
            assertThat(e.getTown()).isEqualTo("Manchester");
        });
    }

    @Test
    @DisplayName("findById returns empty when id does not exist")
    void testFindByIdNotFound() {
        assertThat(repository.findById(999L)).isEmpty();
    }

    @Test
    @DisplayName("count reflects number of persisted rows")
    void testCount() {
        assertThat(repository.count()).isZero();
        entityManager.persistAndFlush(entity);
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Duplicate primary key raises PersistenceException")
    void testDuplicatePrimaryKey() {
        entityManager.persistAndFlush(entity);
        entityManager.clear();

        FemaleDeath duplicate = new FemaleDeath();
        duplicate.setId(2L);
        duplicate.setName("Other");
        duplicate.setDod(LocalDate.of(2025, 4, 1));
        duplicate.setTown("Bristol");

        // persist() forces an INSERT — unlike saveAndFlush which would merge (UPDATE) for an existing id
        // Spring exception translation does not apply to TestEntityManager, so PersistenceException is thrown
        assertThatThrownBy(() -> {
            entityManager.persist(duplicate);
            entityManager.flush();
        }).isInstanceOf(PersistenceException.class);
    }
}
