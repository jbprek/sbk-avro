package foo.kafka.tx.consumer.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.tx.consumer.persistence.BirthStatEntry;
import org.springframework.stereotype.Component;

@Component
public class EventMapper {

    public BirthStatEntry eventToEntity(BirthEvent event) {
        if ( event == null ) {
            return null;
        }

        BirthStatEntry birthStatEntry = new BirthStatEntry();

        birthStatEntry.setId( event.getId() );
        birthStatEntry.setDob( event.getDob() );
        birthStatEntry.setTown( event.getTown() );

        return birthStatEntry;
    }
}
