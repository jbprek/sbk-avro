package foo.kafka.deathevent.service;

import foo.avro.death.DeathEvent;
import foo.kafka.common.MessageCoordinates;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.time.Duration;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProcessorTest {

    @Mock
    EventMapper<DeathEvent, DeathEvent> mapper;

    @Mock
    EventDao<DeathEvent> dao;

    @Mock
    Acknowledgment ack;

    Processor<DeathEvent, DeathEvent> processor;

    @BeforeEach
    void setUp() {
        processor = new Processor<>(mapper, dao);
    }

    @Test
    @DisplayName("On success, processor saves the entity and acknowledges the message")
    void testProcessSuccessShouldSaveAndAcknowledge() {
        DeathEvent payload = mock(DeathEvent.class);

        when(mapper.toEntity(payload)).thenReturn(payload);

        var message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "key-1")
                .build();

        processor.process(message);

        verify(dao, times(1)).save(eq(payload), any(MessageCoordinates.class));
        verify(ack, times(1)).acknowledge();
        verify(ack, never()).nack(ArgumentMatchers.any(Duration.class));
    }

    @Test
    @DisplayName("On transient error, processor nacks the message for retry")
    void testProcessTransientErrorShouldNack() {
        DeathEvent payload = mock(DeathEvent.class);

        when(mapper.toEntity(payload)).thenReturn(payload);
        doThrow(new QueryTimeoutException("transient")).when(dao).save(eq(payload), any(MessageCoordinates.class));

        var message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "key-2")
                .build();

        processor.process(message);

        verify(dao, times(1)).save(eq(payload), any(MessageCoordinates.class));
        verify(ack, never()).acknowledge();
        verify(ack, times(1)).nack(Duration.ofMillis(500));
    }

    @Test
    @DisplayName("On non-transient error, processor acknowledges and skips the message")
    void testProcessNonTransientErrorShouldAckAndSkip() {
        DeathEvent payload = mock(DeathEvent.class);

        when(mapper.toEntity(payload)).thenReturn(payload);
        doThrow(new RuntimeException("fatal")).when(dao).save(eq(payload), any(MessageCoordinates.class));

        var message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "key-3")
                .build();

        processor.process(message);

        verify(dao, times(1)).save(eq(payload), any(MessageCoordinates.class));
        verify(ack, times(1)).acknowledge();
        verify(ack, never()).nack(ArgumentMatchers.any(Duration.class));
    }
}
