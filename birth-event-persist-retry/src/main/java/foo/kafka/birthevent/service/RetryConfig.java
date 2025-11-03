package foo.kafka.birthevent.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.sql.SQLTransientException;
import java.util.Map;

@Configuration
public class RetryConfig {

    @Value("${processor.retry.maxAttempts:5}")
    private int maxAttempts;

    @Value("${processor.retry.initialIntervalMs:200}")
    private long initialIntervalMs;

    @Value("${processor.retry.multiplier:2.0}")
    private double multiplier;

    @Value("${processor.retry.maxIntervalMs:5000}")
    private long maxIntervalMs;

    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        Map<Class<? extends Throwable>, Boolean> retryableExceptions =
        Map.of(TransientDataAccessException.class, true, SQLTransientException.class, true);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(
                3,                 // maxAttempts
                retryableExceptions,
                true               // traverseCauses
        );

        ExponentialBackOffPolicy backOff = new ExponentialBackOffPolicy();
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOff);

        return retryTemplate;
    }
}
