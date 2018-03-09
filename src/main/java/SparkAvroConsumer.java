import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

/**
 * zkServer start
 * cd /usr/local/Cellar/kafka/0.11.0.0/bin
 * kafka-server-start /usr/local/etc/kafka/server.properties
 * kafka-console-producer --broker-list localhost:9092 --topic mytopic
 * kafka-console-consumer --zookeeper localhost:2181 --topic mytopic --from-beginning
 *
 * http://mkuthan.github.io/blog/2016/09/30/spark-streaming-on-yarn/
 *
 * mvn package -DskipTests
 *
 */
public class SparkAvroConsumer {

    private static Injection<GenericRecord, byte[]> recordInjection;

    static {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(SimpleAvroProducer.USER_SCHEMA);
        recordInjection = GenericAvroCodecs.toBinary(schema);
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
            .setAppName("kafka-test")
            .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("mytopic");

        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(
            ssc, String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics
        );

        directKafkaStream
            .map(message -> recordInjection.invert(message._2).get())
            .foreachRDD(rdd -> {
                rdd.foreach(record -> {
                    System.out.println("metric= " + record.get("metric")
                                   + ", measure= " + record.get("measure")
                                   + ", value=" + record.get("value")
                                   + ", ts=" + record.get("ts"));
            });
        });

        // write to hdfs
        Configuration configuration = new Configuration();


        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
