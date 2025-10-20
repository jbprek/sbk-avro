package foo.kafka.tx.consumer.service;

import foo.kafka.tx.consumer.event.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStats;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface EventMapper {
    BirthStats eventToEntity(BirthEvent event);
}
