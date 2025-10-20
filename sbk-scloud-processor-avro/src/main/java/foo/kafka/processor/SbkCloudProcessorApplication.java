package foo.kafka.processor;

import foo.avro.birth.BirthEvent;
import foo.avro.birth.BirthStatEntry;
import foo.kafka.processor.service.Processor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

import org.springframework.messaging.Message;

@SpringBootApplication
@Slf4j
public class SbkCloudProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SbkCloudProcessorApplication.class, args);
    }

//    @Bean
//    public Consumer<Message<BirthEvent>> processBirthEvent(Processor processor) {
//        return processor::process;
//    }

    @Bean
    public Function<Message<BirthEvent>, BirthStatEntry> process(Processor processor) {
         return event -> processor.process(event);
    }

}
