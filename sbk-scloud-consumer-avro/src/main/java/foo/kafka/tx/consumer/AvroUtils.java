package foo.kafka.tx.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@Slf4j
public class AvroUtils {

    private AvroUtils() {
    }
    /** Convert a SpecificRecord to compact Avro JSON (UTF-8). */
    public static String toJsonString(SpecificRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(512);
        writeJson(record, out);
        return out.toString(StandardCharsets.UTF_8);
    }

    /** Core writer: deep-copies with conversions, then uses Avro's jsonEncoder. */
    public static void writeJson(SpecificRecord record, OutputStream out) throws IOException {
        if (record == null) throw new IllegalArgumentException("record is null");

        Schema schema = record.getSchema();
        SpecificData sd = specificDataWithConversions();

        // Deep copy with the SAME SpecificData:
        //  - rewinds ByteBuffer positions (so bytes/decimal aren't empty)
        //  - resolves logical types (Instant, LocalDateTime, BigDecimal, UUID)
        SpecificRecord normalized = (SpecificRecord) sd.deepCopy(schema, record);

        DatumWriter<Object> writer = new SpecificDatumWriter<>(schema, sd);
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
        writer.write(normalized, encoder);
        encoder.flush();
    }

    /** Avro 1.12.0 logical-type conversions. */
    private static SpecificData specificDataWithConversions() {
        var sd = new SpecificData();

        // JSR-310 time types
        sd.addLogicalTypeConversion(new TimeConversions.DateConversion());
        sd.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        sd.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        sd.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        sd.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        sd.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        sd.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());

        // Decimal (bytes/fixed) <-> BigDecimal
        sd.addLogicalTypeConversion(new Conversions.DecimalConversion());

        // UUID (string) <-> java.util.UUID
        sd.addLogicalTypeConversion(new Conversions.UUIDConversion());

        return sd;
    }


}
