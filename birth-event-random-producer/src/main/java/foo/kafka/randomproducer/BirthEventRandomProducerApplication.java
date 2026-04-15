package foo.kafka.randomproducer;

import foo.avro.birth.BirthEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootApplication
@ComponentScan(basePackages = {"foo.kafka.common", "foo.kafka.randomproducer"})
@Slf4j
public class BirthEventRandomProducerApplication {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final int LENGTH = 5;
    private static final Random RANDOM = new Random();
    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static void main(String[] args) {
        SpringApplication.run(BirthEventRandomProducerApplication.class, args);
    }


    @Bean
    public Supplier<Message<BirthEvent>> randomSupplier() {

        return () -> {
            var town = generateRandomTown();
            var event = BirthEvent.newBuilder()
                    .setId(RANDOM.nextLong(1, 10000))
                    .setName(generateRandomName())
                    .setDob(LocalDate.now().minusDays(randomDaysAgo()))
                    .setTown(town)
                    .setWeight(new BigDecimal("3.3"))
                    .setGender(RANDOM.nextBoolean() ? "M" : "F")
                    .build();

            int count = COUNTER.incrementAndGet();
            var msg = MessageBuilder.withPayload(event)
                    .setHeader(KafkaHeaders.KEY, event.getTown())
                    .build();

            log.info("[TX] Producing event #{}: {}", count, event);
            return msg;
        };
    }

    public static String generateRandomName() {
        StringBuilder sb = new StringBuilder(LENGTH);
        for (int i = 0; i < LENGTH; i++) {
            sb.append(CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }

    public static String generateRandomTown() {
        return "TOWN" + RANDOM.nextInt(CHARACTERS.length());
    }

    public static long randomDaysAgo() {
        return (long) RANDOM.nextInt(10) + 1L;
    }

}
