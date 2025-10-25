package foo.kafka.birthevent.consumer;

import foo.avro.birth.BirthEvent;
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
import java.util.function.Function;

@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = {"foo.kafka.common","foo.kafka.birthevent.consumer"})
public class BirthEventSimpleConsumer {

    public static void main(String[] args) {
        SpringApplication.run(BirthEventSimpleConsumer.class, args);
    }

    @Bean
    public Consumer<Message<BirthEvent>> readAndLog() {
        return message -> {
            BirthEvent event = message.getPayload();
            String key = Optional.of(message.getHeaders())
                    .map(headers -> headers.get(KafkaHeaders.RECEIVED_KEY))
                    .map(String.class::cast).orElse(null);
            String coordinates = MessageCoordinates.of(message).toString();
            log.info("Consumed BirthEvent at {}: with key={},value={}", coordinates, key, event);
        };
    }


}
