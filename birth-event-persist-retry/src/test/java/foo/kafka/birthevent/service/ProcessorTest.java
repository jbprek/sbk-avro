package foo.kafka.birthevent.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthMapper;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.ExceptionClassifierRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.sql.SQLTransientException;
import java.util.Collections;

import static org.mockito.Mockito.*;

class ProcessorTest {

    @Test
    void retriesOnTransientException_then_acknowledgesOnce() {
        // Mocks
        BirthMapper mapper = mock(BirthMapper.class);
        BirthDao dao = mock(BirthDao.class);
        Acknowledgment ack = mock(Acknowledgment.class);

        BirthEvent event = new BirthEvent();
        Birth entity = new Birth();

        when(mapper.toEntity(event)).thenReturn(entity);

        // Make dao throw transient exception twice then succeed
        doThrow(new SQLTransientException("transient1"))
                .doThrow(new SQLTransientException("transient2"))
                .doNothing()
                .when(dao).saveBirthEvent(entity);

        // Build a RetryTemplate that mirrors the bean configuration
        RetryTemplate retryTemplate = new RetryTemplate();
        ExceptionClassifierRetryPolicy retryPolicy = new ExceptionClassifierRetryPolicy();
        retryPolicy.setExceptionClassifier((Throwable ex) -> {
            if (ExceptionUtils.isTransient(ex)) {
                return new SimpleRetryPolicy(5, Collections.singletonMap(Exception.class, true));
            }
            return new NeverRetryPolicy();
        });
        FixedBackOffPolicy backOff = new FixedBackOffPolicy();
        backOff.setBackOffPeriod(500L);
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOff);

        Processor processor = new Processor(mapper, dao, retryTemplate);

        Message<BirthEvent> message = MessageBuilder.withPayload(event)
                .setHeader(KafkaHeaders.ACKNOWLEDGMENT, ack)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "k1")
                .build();

        processor.process(message);

        // verify dao called 3 times (2 failures + 1 success)
        verify(dao, times(3)).saveBirthEvent(entity);
        // verify ack acknowledged once
        verify(ack, times(1)).acknowledge();
    }
}
