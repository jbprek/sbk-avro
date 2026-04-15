package foo.kafka.deathevent.consumer;

import foo.avro.death.DeathEvent;
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
@ComponentScan(basePackages = {"foo.kafka.common", "foo.kafka.deathevent.consumer"})
public class DeathEventSimpleConsumer {

    public static void main(String[] args) {
        SpringApplication.run(DeathEventSimpleConsumer.class, args);
    }

    @Bean
    public Consumer<Message<DeathEvent>> readAndLog() {
        return message -> {
            DeathEvent event = message.getPayload();
            String key = Optional.of(message.getHeaders())
                    .map(headers -> headers.get(KafkaHeaders.RECEIVED_KEY))
                    .map(String.class::cast).orElse(null);
            String coordinates = MessageCoordinates.of(message).toString();
            log.info("Consumed DeathEvent at {}: with key={},value={}", coordinates, key, event);
        };
    }

}

