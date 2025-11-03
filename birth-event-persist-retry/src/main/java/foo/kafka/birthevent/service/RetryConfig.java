package foo.kafka.birthevent.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.ExceptionClassifierRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Collections;

@Configuration
public class RetryConfig {

    @Value("${processor.retry.maxAttempts:5}")
    private int maxAttempts;

    @Value("${processor.retry.backoffMs:500}")
    private long backoffMs;

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        ExceptionClassifierRetryPolicy retryPolicy = new ExceptionClassifierRetryPolicy();
        retryPolicy.setExceptionClassifier((Throwable ex) -> {
            if (ExceptionUtils.isTransient(ex)) {
                return new SimpleRetryPolicy(maxAttempts, Collections.singletonMap(Exception.class, true));
            } else {
                return new NeverRetryPolicy();
            }
        });

        FixedBackOffPolicy backOff = new FixedBackOffPolicy();
        backOff.setBackOffPeriod(backoffMs);

        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOff);

        return retryTemplate;
    }
}
