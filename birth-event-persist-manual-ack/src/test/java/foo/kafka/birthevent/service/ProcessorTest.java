package foo.kafka.birthevent.service;


import foo.avro.birth.BirthEvent;
import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
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
    BirthMapper mapper;

    @Mock
    BirthDao dao;

    @Mock
    Acknowledgment ack;

    @InjectMocks
    Processor processor;

    @DisplayName("Tests Processor process method for success, on non-transient and transient errors")
    @Test
    void testProcessSuccessShouldSaveAndAcknowledge() {
        BirthEvent payload = mock(BirthEvent.class);
        Birth entity = mock(Birth.class);

        when(mapper.toEntity(payload)).thenReturn(entity);

        var message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "key-1")
                .build();

        processor.process(message);

        verify(dao, times(1)).saveBirthEvent(entity);
        verify(ack, times(1)).acknowledge();
        verify(ack, never()).nack(ArgumentMatchers.any(Duration.class));
    }

    @DisplayName("Tests Processor process method on transient error should nack")
    @Test
    void testProcessTransientErrorShouldNack() {
        BirthEvent payload = mock(BirthEvent.class);
        Birth entity = mock(Birth.class);

        when(mapper.toEntity(payload)).thenReturn(entity);
        doThrow(new QueryTimeoutException("transient")).when(dao).saveBirthEvent(entity);

        var message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "key-2")
                .build();

        processor.process(message);

        verify(dao, times(1)).saveBirthEvent(entity);
        verify(ack, never()).acknowledge();
        verify(ack, times(1)).nack(Duration.ofMillis(500));
    }

    @DisplayName("Tests Processor process method on non transient error should nack and skip")
    @Test
    void testProcessNonTransientErrorShouldAckAndSkip() {
        BirthEvent payload = mock(BirthEvent.class);
        Birth entity = mock(Birth.class);

        when(mapper.toEntity(payload)).thenReturn(entity);
        doThrow(new RuntimeException("fatal")).when(dao).saveBirthEvent(entity);

        var message = MessageBuilder.withPayload(payload)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "key-3")
                .build();

        processor.process(message);

        verify(dao, times(1)).saveBirthEvent(entity);
        verify(ack, times(1)).acknowledge();
        verify(ack, never()).nack(ArgumentMatchers.any(Duration.class));
    }
}
