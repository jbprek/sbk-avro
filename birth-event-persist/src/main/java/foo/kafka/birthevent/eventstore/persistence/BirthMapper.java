package foo.kafka.birthevent.eventstore.persistence;

import foo.avro.birth.BirthEvent;
import org.mapstruct.*;

@Mapper(unmappedTargetPolicy = ReportingPolicy.IGNORE, componentModel = MappingConstants.ComponentModel.SPRING)
public interface BirthMapper {

    @Mapping(source="registrationTime", target="tm")
    Birth toEntity(BirthEvent birthEvent);

}