package com.pandy.kafka.producer.throughput;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author: Pandy
 * @create: 2022/7/9
 *
 * 提高吞吐量
 **/
public class CustomProducerParameters {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,node1:9092,node2:9092");

        // 自定对应的key和value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 缓冲区大小 32m
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // linger ms 5-100ms, 权衡于批次大小和传输速度
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 压缩 方式
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "pandyyang"));
        }

        kafkaProducer.close();
    }
}
