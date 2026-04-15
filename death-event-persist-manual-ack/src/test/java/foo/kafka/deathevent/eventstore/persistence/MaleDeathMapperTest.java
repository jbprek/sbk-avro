package foo.kafka.deathevent.eventstore.persistence;

import foo.avro.death.DeathEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class MaleDeathMapperTest {

    private MaleDeathMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new MaleDeathMapperImpl();
    }

    @Test
    @DisplayName("All shared fields are mapped from DeathEvent to MaleDeath")
    void testToEntityMapsAllFields() {
        DeathEvent event = DeathEvent.newBuilder()
                .setId(1L).setName("John Smith")
                .setDod(LocalDate.of(2025, 1, 15))
                .setTown("London").setGender("M").build();

        MaleDeath entity = mapper.toEntity(event);

        assertThat(entity.getId()).isEqualTo(1L);
        assertThat(entity.getName()).isEqualTo("John Smith");
        assertThat(entity.getDod()).isEqualTo(LocalDate.of(2025, 1, 15));
        assertThat(entity.getTown()).isEqualTo("London");
    }

    @Test
    @DisplayName("gender from DeathEvent is not mapped (no column in male_deaths)")
    void testGenderIsNotMapped() {
        // MaleDeath extends Death which has no gender field —
        // verify the mapper compiles and runs without error when gender is set
        DeathEvent event = DeathEvent.newBuilder()
                .setId(2L).setName("Bob")
                .setDod(LocalDate.of(2024, 6, 1))
                .setTown("Leeds").setGender("M").build();

        MaleDeath entity = mapper.toEntity(event);

        // entity has no getGender() — compilation itself is the proof;
        // just assert the mapped fields are correct
        assertThat(entity.getId()).isEqualTo(2L);
        assertThat(entity.getName()).isEqualTo("Bob");
    }

    @Test
    @DisplayName("Null fields in DeathEvent are mapped as null in MaleDeath")
    void testNullFieldsRemainNull() {
        DeathEvent event = DeathEvent.newBuilder()
                .setId(3L).setName(null).setDod(null).setTown(null).setGender("M").build();

        MaleDeath entity = mapper.toEntity(event);

        assertThat(entity.getId()).isEqualTo(3L);
        assertThat(entity.getName()).isNull();
        assertThat(entity.getDod()).isNull();
        assertThat(entity.getTown()).isNull();
    }
}
