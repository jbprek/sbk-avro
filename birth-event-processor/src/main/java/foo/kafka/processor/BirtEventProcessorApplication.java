package foo.kafka.processor;

import foo.avro.birth.BirthEvent;
import foo.avro.birth.BirthStatEntry;
import foo.kafka.processor.service.Processor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.messaging.Message;

@SpringBootApplication
@ComponentScan(basePackages = {"foo.kafka.common", "foo.kafka.processor"})
@Slf4j
public class BirtEventProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(BirtEventProcessorApplication.class, args);
    }


    @Bean
    public Function<Message<BirthEvent>, Message<BirthStatEntry>> process(Processor processor) {
         return processor::process;
    }

}
