package foo.kafka.tx.consumer.service;

import foo.kafka.tx.consumer.event.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStats;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class EventMapperTest {
    @Test
    void testBirthEventToBirthOccurrence() {
        EventMapper mapper = new EventMapperImpl();
        BirthEvent birthEvent = new BirthEvent(1L, "John Doe", LocalDate.of(1990, 1, 1), "Springfield");

        BirthStats occurrence = mapper.eventToEntity(birthEvent);

        assertNotNull(occurrence);
        assertEquals(birthEvent.id(), occurrence.getId());
        assertEquals(birthEvent.dob(), occurrence.getDob());
        assertEquals(birthEvent.town(), occurrence.getTown());
    }
}