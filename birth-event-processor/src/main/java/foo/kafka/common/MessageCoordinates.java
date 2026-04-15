package foo.kafka.common;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

@Slf4j
public record MessageCoordinates(String topic, String partition, String offset, Instant timestamp) {

    public static final String INCOMING_COORDINATES_HEADER_NAME =  "x-incoming-coordinates";

    private static final Pattern TO_STRING_PATTERN = Pattern.compile("^(.*)\\((.*)\\s*,\\s*(.*)\\s*,\\s*(.*)\\)$");


    public static MessageCoordinates of(RecordMetadata md) {
        return Optional.ofNullable(md)
                .map(meta -> new MessageCoordinates(meta.topic(),
                        ((Integer)meta.partition()).toString(), ((Long)meta.offset()).toString(),
                        Instant.ofEpochMilli(meta.timestamp())))
                .orElse(null);
    }

    public static MessageCoordinates of(Message<?> message) {
        return Optional.ofNullable(message).map(Message::getHeaders)
                .map(headers -> new MessageCoordinates(
                        (String) headers.get(KafkaHeaders.RECEIVED_TOPIC).toString(),
                        ((Integer) headers.get(KafkaHeaders.RECEIVED_PARTITION)).toString(),
                        ((Long) headers.get(KafkaHeaders.OFFSET)).toString(),
                        Instant.ofEpochMilli((Long) headers.get(KafkaHeaders.RECEIVED_TIMESTAMP))))
                .orElse(null);
    }

    public static MessageCoordinates parse(String s) {
        if (s == null) {
            return null;
        }
        Matcher m = TO_STRING_PATTERN.matcher(s.trim());
        if (!m.matches()) {
            log.debug("MessageCoordinates String '{}' does not match expected format", s);
            return null;
        }

        String topicPart = m.group(1);
        String partitionPart = m.group(2).trim();
        String offsetPart = m.group(3).trim();
        String timestampPart = m.group(4).trim();

        Instant parsedTimestamp;
        try {
            parsedTimestamp = Instant.parse(timestampPart);
        } catch (Exception e) {
            log.debug("MessageCoordinates timestamp '{}' could not be parsed as Instant", timestampPart);
            parsedTimestamp = null;
        }

        return new MessageCoordinates(
                Objects.requireNonNullElse(topicPart, "null"),
                Objects.requireNonNullElse(partitionPart, "-1"),
                Objects.requireNonNullElse(offsetPart, "-1"),
                parsedTimestamp);
    }

    @Override
    public String toString() {
        return String.format("%s(%s,%s,%s)",
                Objects.toString(topic(), "null"),
                Objects.toString(partition(), "-1"),
                Objects.toString(offset(), "-1"),
                Objects.toString(timestamp(), "null"));
    }
}
