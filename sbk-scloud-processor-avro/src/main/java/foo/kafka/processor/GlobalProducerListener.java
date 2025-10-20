package foo.kafka.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GlobalProducerListener implements ProducerListener<Object, Object> {

    @Override
    public void onSuccess(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata) {
        if (recordMetadata != null) {
            log.info("[TX-LISTENER] Sent to topic={} partition={} offset={} key={} value={}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), producerRecord != null ? producerRecord.key() : null, producerRecord != null ? producerRecord.value() : null);
        } else if (producerRecord != null) {
            log.info("[TX-LISTENER] Sent to topic={} key={}", producerRecord.topic(), producerRecord.key());
        }
    }

    @Override
    public void onError(ProducerRecord<Object, Object> producerRecord, @Nullable RecordMetadata recordMetadata, Exception exception) {
        log.error("[TX-LISTENER] Error sending to topic={} partition={} key={}", producerRecord != null ? producerRecord.topic() : null, recordMetadata != null ? recordMetadata.partition() : null, producerRecord != null ? producerRecord.key() : null, exception);
    }
}
