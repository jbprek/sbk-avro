package foo.kafka.deathevent.eventstore.persistence;

import foo.avro.death.DeathEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class FemaleDeathMapperTest {

    private FemaleDeathMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new FemaleDeathMapperImpl();
    }

    @Test
    @DisplayName("All shared fields are mapped from DeathEvent to FemaleDeath")
    void testToEntityMapsAllFields() {
        DeathEvent event = DeathEvent.newBuilder()
                .setId(2L).setName("Jane Smith")
                .setDod(LocalDate.of(2025, 3, 20))
                .setTown("Manchester").setGender("F").build();

        FemaleDeath entity = mapper.toEntity(event);

        assertThat(entity.getId()).isEqualTo(2L);
        assertThat(entity.getName()).isEqualTo("Jane Smith");
        assertThat(entity.getDod()).isEqualTo(LocalDate.of(2025, 3, 20));
        assertThat(entity.getTown()).isEqualTo("Manchester");
    }

    @Test
    @DisplayName("gender from DeathEvent is not mapped (no column in female_deaths)")
    void testGenderIsNotMapped() {
        // FemaleDeath extends Death which has no gender field —
        // verify the mapper compiles and runs without error when gender is set
        DeathEvent event = DeathEvent.newBuilder()
                .setId(3L).setName("Alice")
                .setDod(LocalDate.of(2024, 9, 10))
                .setTown("Bristol").setGender("F").build();

        FemaleDeath entity = mapper.toEntity(event);

        // entity has no getGender() — compilation itself is the proof;
        // just assert the mapped fields are correct
        assertThat(entity.getId()).isEqualTo(3L);
        assertThat(entity.getName()).isEqualTo("Alice");
    }

    @Test
    @DisplayName("Null fields in DeathEvent are mapped as null in FemaleDeath")
    void testNullFieldsRemainNull() {
        DeathEvent event = DeathEvent.newBuilder()
                .setId(4L).setName(null).setDod(null).setTown(null).setGender("F").build();

        FemaleDeath entity = mapper.toEntity(event);

        assertThat(entity.getId()).isEqualTo(4L);
        assertThat(entity.getName()).isNull();
        assertThat(entity.getDod()).isNull();
        assertThat(entity.getTown()).isNull();
    }
}
