package foo.kafka.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

@Component
@Slf4j
public class GlobalProducerListener implements ProducerListener<Object, Object> {

    @Override
    public void onSuccess(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata) {
        if (producerRecord == null || recordMetadata == null) {
            log.warn("[TX-LISTENER] onSuccess called with null RecordMetadata for producerRecord={}", producerRecord);
            return;
        }
        var coordinates = MessageCoordinates.of(recordMetadata);

        String incomingCoordinatesHeaderValue = Optional.ofNullable(producerRecord.headers())
                .map(headers -> headers.lastHeader(MessageCoordinates.INCOMING_COORDINATES_HEADER_NAME))
                .map(header -> new String(header.value(), StandardCharsets.UTF_8))
                .orElse(null);

        var inputCoordinates = MessageCoordinates.parse(incomingCoordinatesHeaderValue);


//        var inputCoordinates = MessageCoordinates.of(producerRecord);
        var key = Optional.ofNullable(producerRecord).map(ProducerRecord::key).orElse(null);
        var value = Optional.ofNullable(producerRecord).map(ProducerRecord::value).orElse(null);
            log.info("[TX-LISTENER] Processed {},  Sent to {} key={} value={}",
                    inputCoordinates,
                    coordinates,
                    key,
                    value);
    }

    @Override
    public void onError(ProducerRecord<Object, Object> producerRecord, @Nullable RecordMetadata recordMetadata, Exception exception) {
        var coordinates = MessageCoordinates.of(recordMetadata);
        var key = Optional.ofNullable(producerRecord).map(ProducerRecord::key).orElse(null);
        log.error("[TX-LISTENER] Error sending to {} key={}, error: {} ",coordinates, key, exception.getMessage(), exception);
    }
}
