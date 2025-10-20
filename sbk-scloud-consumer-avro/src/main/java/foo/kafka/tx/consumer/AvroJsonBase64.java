package foo.kafka.tx.consumer;

import org.apache.avro.*;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public final class AvroJsonBase64 {
    private AvroJsonBase64() {}

    /** Public entry point (string). */
    public static String toJson(SpecificRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(512);
        write(record, out);
        return out.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Public entry point (bytes). */
    public static byte[] toJsonBytes(SpecificRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(512);
        write(record, out);
        return out.toByteArray();
    }

    /** Core writer. */
    public static void write(SpecificRecord record, OutputStream out) throws Exception {
        if (record == null) throw new IllegalArgumentException("record is null");
        SpecificData sd = specificData();
        // normalize (rewinds ByteBuffers, resolves logicals for deep copies)
        SpecificRecord normalized = (SpecificRecord) sd.deepCopy(record.getSchema(), record);

        JsonFactory jf = new JsonFactory();
        try (JsonGenerator g = jf.createGenerator(out)) {
            writeValue(g, sd, record.getSchema(), normalized);
            g.flush();
        }
    }

    // ---- schema-walking writer ----

    private static void writeValue(JsonGenerator g, SpecificData sd, Schema schema, Object value) throws Exception {
        if (value == null) { g.writeNull(); return; }

        // Union: resolve active branch AND write with wrapper name
        if (schema.getType() == Schema.Type.UNION) {
            Schema branch = sd.resolveUnion(schema, value);
            g.writeStartObject();
            g.writeFieldName(branch.getType().getName());
            writeValue(g, sd, branch, value);
            g.writeEndObject();
            return;
        }

        // Logical types (timestamps, date/time, decimal, uuid)
        LogicalType lt = schema.getLogicalType();
        if (lt != null) {
            if (lt instanceof LogicalTypes.TimestampMillis || lt instanceof LogicalTypes.TimestampMicros) {
                // Allow Instant, Long, or CharSequence epoch input
                long epoch = (value instanceof Instant) ? ((Instant) value).toEpochMilli()
                        : (value instanceof Long) ? (Long) value
                        : Long.parseLong(value.toString());
                g.writeNumber(lt instanceof LogicalTypes.TimestampMicros ? epoch : epoch); // epoch number
                return;
            }
            if (lt instanceof LogicalTypes.Date) { // int days
                int days = (value instanceof Integer) ? (Integer) value : Integer.parseInt(value.toString());
                g.writeNumber(days);
                return;
            }
            if (lt instanceof LogicalTypes.TimeMillis || lt instanceof LogicalTypes.TimeMicros) {
                long t = (value instanceof Integer) ? ((Integer) value).longValue()
                        : (value instanceof Long) ? (Long) value
                        : Long.parseLong(value.toString());
                g.writeNumber(t);
                return;
            }
            if (lt instanceof LogicalTypes.LocalTimestampMillis || lt instanceof LogicalTypes.LocalTimestampMicros) {
                long t = (value instanceof Long) ? (Long) value : Long.parseLong(value.toString());
                g.writeNumber(t);
                return;
            }
            if (lt instanceof LogicalTypes.Decimal) {
                // Always output Base64 of two's-complement big-endian unscaled value
                byte[] bytes = toDecimalBytes(sd, (LogicalTypes.Decimal) lt, schema, value);
                g.writeString(Base64.getEncoder().encodeToString(bytes));
                return;
            }
            if (lt instanceof LogicalTypes.Uuid) {
                g.writeString(value.toString());
                return;
            }
        }

        // Primitives & composites
        switch (schema.getType()) {
            case NULL: g.writeNull(); return;
            case BOOLEAN: g.writeBoolean((Boolean) value); return;
            case INT: g.writeNumber(value instanceof Integer ? (Integer) value : Integer.parseInt(value.toString())); return;
            case LONG: g.writeNumber(value instanceof Long ? (Long) value : Long.parseLong(value.toString())); return;
            case FLOAT: g.writeNumber((Float) value); return;
            case DOUBLE: g.writeNumber((Double) value); return;
            case STRING: g.writeString(value.toString()); return;

            case BYTES: {
                ByteBuffer buf = (value instanceof ByteBuffer) ? ((ByteBuffer) value).duplicate()
                        : ByteBuffer.wrap((byte[]) value);
                byte[] b = new byte[buf.remaining()];
                buf.get(b);
                g.writeString(Base64.getEncoder().encodeToString(b));
                return;
            }

            case FIXED: {
                byte[] b = (value instanceof GenericFixed) ? ((GenericFixed) value).bytes() : (byte[]) value;
                g.writeString(Base64.getEncoder().encodeToString(b));
                return;
            }

            case RECORD: {
                g.writeStartObject();
                List<Schema.Field> fields = schema.getFields();
                for (int i = 0; i < fields.size(); i++) {
                    Schema.Field f = fields.get(i);
                    g.writeFieldName(f.name());
                    Object v = (value instanceof SpecificRecord)
                            ? ((SpecificRecord) value).get(i)
                            : ((GenericData.Record) value).get(i);
                    writeValue(g, sd, f.schema(), v);
                }
                g.writeEndObject();
                return;
            }

            case ARRAY: {
                g.writeStartArray();
                for (Object item : (List<?>) value) {
                    writeValue(g, sd, schema.getElementType(), item);
                }
                g.writeEndArray();
                return;
            }

            case MAP: {
                g.writeStartObject();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
                    g.writeFieldName(e.getKey().toString());
                    writeValue(g, sd, schema.getValueType(), e.getValue());
                }
                g.writeEndObject();
                return;
            }

            case ENUM: g.writeString(value.toString()); return;

            default:
                throw new IllegalArgumentException("Unsupported type: " + schema);
        }
    }

    private static byte[] toDecimalBytes(SpecificData sd, LogicalTypes.Decimal dec, Schema schema, Object value) {
        // Accept BigDecimal, byte[], ByteBuffer, GenericFixed
        if (value instanceof BigDecimal) {
            DecimalConversion conv = new DecimalConversion();
            return conv.toBytes((BigDecimal) value, schema, dec).array();
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer dup = ((ByteBuffer) value).duplicate();
            byte[] out = new byte[dup.remaining()];
            dup.get(out);
            return out;
        }
        if (value instanceof GenericFixed) {
            return ((GenericFixed) value).bytes();
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        // Fallback: parse from string (e.g., "12.3")
        DecimalConversion conv = new DecimalConversion();
        return conv.toBytes(new BigDecimal(value.toString()), schema, dec).array();
    }

    private static SpecificData specificData() {
        SpecificData sd = new SpecificData();
        // Register all relevant logical conversions (Avro 1.12.0)
        sd.addLogicalTypeConversion(new TimeConversions.DateConversion());
        sd.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        sd.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        sd.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        sd.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        sd.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        sd.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
        sd.addLogicalTypeConversion(new Conversions.DecimalConversion());
        sd.addLogicalTypeConversion(new Conversions.UUIDConversion());
        return sd;
    }
}