package net.yjx.test01;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","dsj02:9092,dsj03:9092,dsj04:9092");

        //todo 反序列化
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        //TODO 消费者组
        props.put("group.id","testGroup01");

        //todo 指定为自动提交
        props.put("enable.auto.commit", "true");//默认值true
        props.put("auto.commit.interval.ms", "1000");//默认值5000

        //创建consumer对象
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("topic","topic0818-test"));

        //todo 从topic中消费数据
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(2000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition=%d , offset=%d , key=%s , val=%s%n",
                        record.partition(),record.offset(),
                        record.key(),record.value());
            }
        }

    }
}
