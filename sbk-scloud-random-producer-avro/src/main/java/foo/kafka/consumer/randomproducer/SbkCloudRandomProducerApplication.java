package foo.kafka.consumer.randomproducer;

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

import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootApplication
@Slf4j
public class SbkCloudRandomProducerApplication {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final int LENGTH = 5;
    private static final Random RANDOM = new Random();
    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static void main(String[] args) {
        SpringApplication.run(SbkCloudRandomProducerApplication.class, args);
    }


    @Bean
    public Supplier<Message<BirthEvent>> randomSupplier() {
        // Build a new event and wrap it into a Message each time the supplier is invoked
        // so Cloud Stream's poller will produce a new message on each poll instead of the same first event.
        return () -> {
            var event = BirthEvent.newBuilder()
                    .setId(RANDOM.nextLong(1, 10000))
                    .setName(generateRandomName())
                    .setDob(LocalDate.now().minusDays(randomDaysAgo()))
                    .setTown("Town")
                    .setWeight(new BigDecimal("3.3"))
                    .build();

            int count = COUNTER.incrementAndGet();
            var msg = MessageBuilder.withPayload(event)
                    .setHeader("sequence", count)
                    .build();

            log.info("[TX] Produced event #{}: {}", count, event);
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

    public static long randomDaysAgo() {
        return (long) RANDOM.nextInt(10) + 1L;
    }

}
