package Demo;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
//import kafka.javaapi.consumer.ConsumerConnector;


public class LocalConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("group.id", args[1]);
//        props.put("zookeeper.connect","host00:2181/kafka1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(args[2], "global22"));
        while (true) {
//            System.out.printf("fsdf\n");
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }
    }
}
