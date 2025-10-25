package foo.kafka.processor.service;

import foo.avro.birth.BirthEvent;
import foo.avro.birth.BirthStatEntry;
import org.springframework.stereotype.Component;

@Component
public class EventMapper {

    public BirthStatEntry eventToStatEntry(BirthEvent event) {
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
