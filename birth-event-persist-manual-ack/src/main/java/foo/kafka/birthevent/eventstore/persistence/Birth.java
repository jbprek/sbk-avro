package foo.kafka.birthevent.eventstore.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

@Getter
@Setter
@Entity
@Table(name = "births")
public class Birth {
    @Id
    @NotNull
    @Column(name = "reg_id", nullable = false)
    private Long id;

    @NotNull
    @Size(max = 100)
    @Column(name = "name", length = 100, nullable = false)
    private String name;

    @NotNull
    @Column(name = "dob", nullable = false)
    private LocalDate dob;

    @NotNull
    @Size(max = 50)
    @Column(name = "town", length = 50, nullable = false)
    private String town;

    @NotNull
    @Column(name = "reg_time", nullable = false)
    private Instant regTime;

    @NotNull
    @Column(name = "weight", precision = 3, scale = 1, nullable = false)
    private BigDecimal weight;

    @Size(max = 1)
    @NotNull
    @Pattern(regexp = "[MF]")
    @Column(name = "gender", length = 1, nullable = false)
    private String gender;

}