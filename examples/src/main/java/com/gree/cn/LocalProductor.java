package com.gree.cn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @Description:
 * @Author: greenday
 * @Date: 2020/1/7 10:37
 */
public class LocalProductor {
    public static void main(String[] args) {
        Properties pro = new Properties();
        pro.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,Propertise.SERVER_URL);
        pro.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        pro.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(pro);
        int key = 1;
        while (true){
            ProducerRecord<String, String> record = new ProducerRecord<>(Propertise.TOPIC, null, "message:num-" + key);
            System.out.println("("+record.key()+":"+record.value());
            Future<RecordMetadata> send = producer.send(record);
            key++;
        }
    }
}
