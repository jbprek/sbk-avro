package foo.kafka.deathevent.eventstore.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;

@Getter
@Setter
@MappedSuperclass
public abstract class Death {

    @Id
    @Column(name = "reg_id", nullable = false)
    private Long id;

    @Size(max = 100)
    @Column(name = "name", length = 100)
    private String name;

    @Column(name = "dod")
    private LocalDate dod;

    @Size(max = 50)
    @Column(name = "town", length = 50)
    private String town;
}

