package foo.kafka.deathevent.eventstore.persistence;

import foo.avro.death.DeathEvent;
import foo.kafka.deathevent.service.EventMapper;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(unmappedTargetPolicy = ReportingPolicy.IGNORE, componentModel = MappingConstants.ComponentModel.SPRING)
public interface FemaleDeathMapper extends EventMapper<DeathEvent, FemaleDeath> {

    FemaleDeath toEntity(DeathEvent deathEvent);
}

