package bigdataHbase.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerUtil {

    public static Properties proProducer(){
        Properties props=new Properties();
        props.put("bootstrap.servers", "10.1.69.11:6667");
        props.put("acks", "all");
        props.put("retries ", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;

    }

    public static void main(String []args){
        Producer<String, String> producer = new KafkaProducer <String, String>(proProducer());
        for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("myTopic", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }
}
