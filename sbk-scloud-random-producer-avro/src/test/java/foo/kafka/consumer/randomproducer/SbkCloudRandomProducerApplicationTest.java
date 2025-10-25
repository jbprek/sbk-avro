package foo.kafka.consumer.randomproducer;

import foo.avro.birth.BirthEvent;
import org.junit.jupiter.api.Test;
import org.assertj.core.api.Assertions;
import org.springframework.messaging.Message;

import java.util.function.Supplier;

class SbkCloudRandomProducerApplicationTest {

    @Test
    void generateRandomName_length_and_chars() {
        String name = SbkCloudRandomProducerApplication.generateRandomName();
        Assertions.assertThat(name).isNotNull();
        Assertions.assertThat(name).hasSize(5);
        Assertions.assertThat(name).matches("^[A-Za-z]{5}$");
    }

    @Test
    void randomDaysAgo_range() {
        long days = SbkCloudRandomProducerApplication.randomDaysAgo();
        Assertions.assertThat(days).isBetween(1L, 10L);
    }

    @Test
    void supplier_returns_message_with_payload_and_sequence_increasing() {
        SbkCloudRandomProducerApplication app = new SbkCloudRandomProducerApplication();
        Supplier<Message<BirthEvent>> supplier = app.randomSupplier();
        Message<BirthEvent> m1 = supplier.get();
        Message<BirthEvent> m2 = supplier.get();
        Assertions.assertThat(m1).isNotNull();
        Assertions.assertThat(m1.getPayload()).isInstanceOf(BirthEvent.class);
        Object seq1Obj = m1.getHeaders().get("sequence");
        Object seq2Obj = m2.getHeaders().get("sequence");
        Assertions.assertThat(seq1Obj).isNotNull();
        Assertions.assertThat(seq2Obj).isNotNull();
        // headers are created as Integer by supplier
        int seq1 = ((Number) seq1Obj).intValue();
        int seq2 = ((Number) seq2Obj).intValue();
        Assertions.assertThat(seq2).isGreaterThan(seq1);
    }
}
