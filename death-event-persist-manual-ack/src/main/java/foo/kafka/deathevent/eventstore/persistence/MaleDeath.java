package foo.kafka.deathevent.eventstore.persistence;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
@Entity
@Table(name = "male_deaths")
public class MaleDeath extends Death {
}

