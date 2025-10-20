package foo.kafka.tx.consumer.event;

import java.time.LocalDate;

public record BirthEvent(Long id, String name, LocalDate dob, String town) {
}