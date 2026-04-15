package foo.kafka.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.lang.Nullable;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
public class GlobalProducerListener implements ProducerListener<Object, Object> {

    @Override
    public void onSuccess(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata) {
        var coordinates = MessageCoordinates.of(recordMetadata);
        var key = Optional.ofNullable(producerRecord).map(ProducerRecord::key).orElse(null);
        var value = Optional.ofNullable(producerRecord).map(ProducerRecord::value).orElse(null);
            log.info("[TX-LISTENER] Sent to {} key={} value={}",
                    coordinates,
                    key,
                    value);
    }

    @Override
    public void onError(ProducerRecord<Object, Object> producerRecord, @Nullable RecordMetadata recordMetadata, Exception exception) {
        var coordinates = MessageCoordinates.of(recordMetadata);
        var key = Optional.ofNullable(producerRecord).map(ProducerRecord::key).orElse(null);
        log.error("[TX-LISTENER] Error sending to {} key={}, error: {} ", coordinates, key, exception.getMessage(), exception);
    }
}

