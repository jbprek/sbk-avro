package foo.kafka.birthevent.eventstore.persistence;

import foo.avro.birth.BirthEvent;
import foo.avro.birth.Gender;
import foo.kafka.birthevent.service.EventMapper;
import org.mapstruct.*;

@Mapper(unmappedTargetPolicy = ReportingPolicy.IGNORE, componentModel = MappingConstants.ComponentModel.SPRING)
public interface BirthMapper extends EventMapper<BirthEvent, Birth> {

    @Mapping(source = "registrationTime", target = "regTime")
    @Mapping(source = "gender", target = "gender", qualifiedByName = "genderToString")
    Birth toEntity(BirthEvent birthEvent);

    @Named("genderToString")
    default String genderToString(Gender gender) {
        if (gender == null) return null;
        return switch (gender) {
            case MALE -> "M";
            case FEMALE -> "F";
        };
    }

}