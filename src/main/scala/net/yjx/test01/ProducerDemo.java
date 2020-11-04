package net.yjx.test01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","dsj02:9092,dsj03:9092,dsj04:9092");
        props.put("ack","all");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //StringSerializer
//        props.put("retries",0);
//        props.put("batch.size",16384);
        
        //todo 创建kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //todo
        for (int i=0;i<1000;i++){
            producer.send(new ProducerRecord<>("topic081901",Integer.toString(i),"value"+i));
        }

        producer.close();

    }
}
