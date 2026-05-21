package foo.kafka.test.common;


import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.File;
import java.io.FileInputStream;

public class AvroResourceLoader {

    private AvroResourceLoader() {
        // Private constructor to prevent instantiation
    }

    public static <T extends SpecificRecord> T convertJsonToAvro(File input, Class<T> avroClazz) {
       try {
           DatumReader<T> datumReader = new SpecificDatumReader<>(avroClazz);
           var schema = (Schema) avroClazz.getMethod("getClassSchema").invoke(null);

           Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new FileInputStream(input));
           return datumReader.read(null, decoder);
       } catch(Exception e) {
           throw new RuntimeException("Failed to convert JSON to Avro", e);
       }
    }
}
