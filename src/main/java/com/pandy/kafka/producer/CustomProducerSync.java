package com.pandy.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author: Pandy
 * @create: 2022/6/12
 **/
public class CustomProducerSync {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,node1:9092,node2:9092");

        // 自定对应的key和value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 1.创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 2.发送数据
        for (int i = 0; i < 5; i++) {

            // 同步发送 get就是等待future调用后的结果
            kafkaProducer.send(new ProducerRecord<>("first", "pandy" + i)).get();
        }
        // 3 关闭连接
        kafkaProducer.close();
    }
}
