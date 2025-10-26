package foo.kafka.birtgstats.consumer;


import foo.avro.birth.BirthStatEntry;
import foo.kafka.common.MessageCoordinates;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = {"foo.kafka.common","foo.kafka.birtgstats.consumer"})
public class BirthStatSimpleConsumer {

    public static void main(String[] args) {
        SpringApplication.run(BirthStatSimpleConsumer.class, args);
    }

    @Bean
    public Consumer<Message<BirthStatEntry>> readAndLog() {
        return message -> {
            BirthStatEntry event = message.getPayload();
            String key = Optional.of(message.getHeaders())
                    .map(headers -> headers.get(KafkaHeaders.RECEIVED_KEY))
                    .map(String.class::cast).orElse(null);
            String coordinates = MessageCoordinates.of(message).toString();
            log.info("Consumed BirthStatEntry at {}: with key={},value={}", coordinates, key, event);
        };
    }


}
