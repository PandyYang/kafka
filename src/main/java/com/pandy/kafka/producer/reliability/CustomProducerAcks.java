package com.pandy.kafka.producer.reliability;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author: Pandy
 * @create: 2022/7/9
 *
 * 可靠性的保证
 *
 * 1. acks=0，生产者发送过来数据就不管了，可靠性差，效率高；
 * 2. acks=1，生产者发送过来数据Leader应答，可靠性中等，效率中等；
 * 3. acks=-1，生产者发送过来数据Leader和ISR队列里面所有Follower应答，可靠性高，效率低；
 *
 **/
public class CustomProducerAcks {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,node1:9092,node2:9092");

        // 自定对应的key和value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 应答等级
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");

        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

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
