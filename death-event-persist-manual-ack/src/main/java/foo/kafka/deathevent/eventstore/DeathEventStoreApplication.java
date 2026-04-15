package foo.kafka.deathevent.eventstore;

import foo.avro.death.DeathEvent;
import foo.kafka.deathevent.service.DeathDao;
import foo.kafka.deathevent.service.Processor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.messaging.Message;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.function.Consumer;

@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = {"foo.kafka.common", "foo.kafka.deathevent"})
@EnableJpaRepositories(basePackages = "foo.kafka.deathevent.eventstore.persistence")
@EntityScan(basePackages = "foo.kafka.deathevent.eventstore.persistence")
@EnableTransactionManagement
public class DeathEventStoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeathEventStoreApplication.class, args);
    }

    @Bean
    public Processor<DeathEvent> deathEventProcessor(DeathDao dao) {
        return new Processor<>(dao);
    }

    @Bean
    public Consumer<Message<DeathEvent>> process(Processor<DeathEvent> processor) {
        return processor::process;
    }
}

