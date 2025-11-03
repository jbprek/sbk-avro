package foo.kafka.birthevent.service;

import foo.avro.birth.BirthEvent;
import foo.kafka.birthevent.eventstore.persistence.Birth;
import foo.kafka.birthevent.eventstore.persistence.BirthMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.ExceptionClassifierRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collections;

import static org.mockito.Mockito.*;

class ProcessorTest {
    @Mock
    BirthMapper mapper = mock(BirthMapper.class);
    @Mock
    BirthDao dao = mock(BirthDao.class);

    BirthEvent event = new BirthEvent();
    Birth entity = new Birth();


    @DisplayName("Tests that the Processor retries on transient exceptions and persists once upon success")
    @Test
    void retriesOnTransientException() {

        when(mapper.toEntity(event)).thenReturn(entity);

        // Make dao throw transient exception twice then succeed
        doThrow(new QueryTimeoutException("transient1"))
                .doThrow(new QueryTimeoutException("transient2"))
                .doNothing()
                .when(dao).saveBirthEvent(entity);

        // Build a RetryTemplate that mirrors the bean configuration
        RetryTemplate retryTemplate = buildRetryTemplate();

        Processor processor = new Processor(mapper, dao, retryTemplate);

        Message<BirthEvent> message = MessageBuilder.withPayload(event)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "k1")
                .build();

        processor.process(message);

        // verify dao called 3 times (2 failures + 1 success)
        verify(dao, times(3)).saveBirthEvent(entity);
    }

    @DisplayName("Tests that the Processor does not retry on non-transient exceptions")
    @Test
    void noRetryOnNonTransientException() {

        when(mapper.toEntity(event)).thenReturn(entity);

        doThrow(new DataIntegrityViolationException("Non Transient"))
                .when(dao).saveBirthEvent(entity);

        RetryTemplate retryTemplate = buildRetryTemplate();

        Processor processor = new Processor(mapper, dao, retryTemplate);

        Message<BirthEvent> message = MessageBuilder.withPayload(event)
                .setHeader(KafkaHeaders.RECEIVED_KEY, "k1")
                .build();

        processor.process(message);

        // verify dao called 3 times (2 failures + 1 success)
        verify(dao, times(1)).saveBirthEvent(entity);
    }


    // Helper to construct the RetryTemplate used by the Processor in tests
    private RetryTemplate buildRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        ExceptionClassifierRetryPolicy retryPolicy = new ExceptionClassifierRetryPolicy();
        retryPolicy.setExceptionClassifier((Throwable ex) -> {
            if (ExceptionUtils.isTransient(ex)) {
                return new SimpleRetryPolicy(5, Collections.singletonMap(Exception.class, true));
            } else {
                return new NeverRetryPolicy();
            }
        });
        FixedBackOffPolicy backOff = new FixedBackOffPolicy();
        backOff.setBackOffPeriod(500L);
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOff);
        return retryTemplate;
    }
}
