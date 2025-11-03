package foo.kafka.birthevent.eventstore;

import foo.avro.birth.BirthEvent;
import foo.kafka.birthevent.service.Processor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = {"foo.kafka.common","foo.kafka.birthevent"})
// Ensure JPA repositories and JPA entities under the eventstore package are picked up
@EnableJpaRepositories(basePackages = "foo.kafka.birthevent.eventstore.persistence")
@EntityScan(basePackages = "foo.kafka.birthevent.eventstore.persistence")
@EnableTransactionManagement
public class BirthEventStoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(BirthEventStoreApplication.class, args);
    }

    @Bean
    public Consumer<Message<BirthEvent>> process(Processor processor) {
        return processor::process;
    }

}
