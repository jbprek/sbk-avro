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
class MaleDeathRepositoryTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private MaleDeathRepository repository;

    private MaleDeath entity;

    @BeforeEach
    void setUp() {
        entity = new MaleDeath();
        entity.setId(1L);
        entity.setName("John Smith");
        entity.setDod(LocalDate.of(2025, 1, 15));
        entity.setTown("London");
    }

    @Test
    @DisplayName("Saved MaleDeath can be retrieved by id")
    void testSaveAndFindById() {
        entityManager.persistAndFlush(entity);
        entityManager.clear();

        var found = repository.findById(1L);

        assertThat(found).isPresent().hasValueSatisfying(e -> {
            assertThat(e.getId()).isEqualTo(1L);
            assertThat(e.getName()).isEqualTo("John Smith");
            assertThat(e.getDod()).isEqualTo(LocalDate.of(2025, 1, 15));
            assertThat(e.getTown()).isEqualTo("London");
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

        MaleDeath duplicate = new MaleDeath();
        duplicate.setId(1L);
        duplicate.setName("Other");
        duplicate.setDod(LocalDate.of(2025, 2, 1));
        duplicate.setTown("Leeds");

        // persist() forces an INSERT — unlike saveAndFlush which would merge (UPDATE) for an existing id
        // Spring exception translation does not apply to TestEntityManager, so PersistenceException is thrown
        assertThatThrownBy(() -> {
            entityManager.persist(duplicate);
            entityManager.flush();
        }).isInstanceOf(PersistenceException.class);
    }
}
