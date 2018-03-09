import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * mvn install -DskipTests
 * java -cp target/lib/*:target/spark-example.jar SimpleAvroProducer
 */
public class SimpleAvroProducer {

    public static final String USER_SCHEMA = "{"
        + "\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":["
        + "  { \"name\":\"metric\", \"type\":\"string\" },"
        + "  { \"name\":\"measure\", \"type\":\"string\" },"
        + "  { \"name\":\"value\", \"type\":\"int\" },"
        + "  { \"name\":\"ts\", \"type\":{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" } }"
        + "]}";

    public static final List<String> metricList = new ArrayList<String>(
        Arrays.asList("plant1", "plant2", "plant3")
    );

    public static final List<String> measureList = new ArrayList<String>(
        Arrays.asList("pressure", "temp", "speed")
    );

    public static final int min = 0;
    public static final int max = 100;

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        Random randomizer = new Random();

        for (int i = 0; i < 1000; i++) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("metric", metricList.get(randomizer.nextInt(metricList.size())));
            avroRecord.put("measure", measureList.get(randomizer.nextInt(measureList.size())));
            avroRecord.put("value", ThreadLocalRandom.current().nextInt(min, max + 1));
            avroRecord.put("ts", Instant.now().toEpochMilli());

            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("mytopic", bytes);
            producer.send(record);

            Thread.sleep(250);
        }

        producer.close();
    }

}
