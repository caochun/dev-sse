package Demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class LocalProducer {
    private String server = "";
    private String topic = "";
    private Properties props = null;
    Producer<String, String> producer = null;

    public LocalProducer(String server, String topic){
        this.server = server;
        this.topic = topic;
        props = new Properties();
        props.put("bootstrap.servers", this.server);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    public void send(String key, String value){
        producer.send(new ProducerRecord<String, String>(this.topic, key, value));

    }
    public void close(){
        producer.close();
    }
}
