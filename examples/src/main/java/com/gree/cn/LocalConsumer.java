package com.gree.cn;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

/**
 * @Description:kafak消费者
 * @Author: greenday
 * @Date: 2020/1/2 17:13
 */
public class LocalConsumer {
    public static void main(String[] args) {
        Properties pro = new Properties();
        pro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Propertise.SERVER_URL);
        pro.setProperty(ConsumerConfig.GROUP_ID_CONFIG,Propertise.GROUP_ID);
        pro.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        pro.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        pro.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer<String,String> consumer = new KafkaConsumer(pro);
        consumer.assign(Collections.singletonList(new TopicPartition(Propertise.TOPIC,0)));
        consumer.subscribe(Collections.singletonList(Propertise.TOPIC));
//        consumer.assign(Collections.singletonList(new TopicPartition(Propertise.TOPIC,0)));
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(Propertise.TOPIC);
//        consumer.seek(new TopicPartition(Propertise.TOPIC,0),500);
        while (true){
//            柱塞一段时间，这段时间尽可能多的去拉取数据
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String,String> record : records) {
                System.out.println("k:"+record.key()+" v: " +record.value()+" p: "+record.partition()+" offset: "+record.offset() );
            }
        }
    }
}
