package foo.kafka.processor.service;

import foo.avro.birth.BirthEvent;
import foo.avro.birth.BirthStatEntry;
import foo.kafka.common.MessageCoordinates;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class Processor {
    private final EventMapper eventMapper;


    public Message<BirthStatEntry>  process(Message<BirthEvent> message) {
        var coordinates = MessageCoordinates.of(message);
        log.info("[TX] Processing event  at: {} : key {}, value: {})", coordinates, message.getHeaders().get("key"), message.getPayload());
        var statEntry =  eventMapper.eventToStatEntry(message.getPayload());

        return MessageBuilder
                .withPayload(statEntry)
                .setHeader(KafkaHeaders.KEY, coordinates.toString())
                .build();
    }
}
