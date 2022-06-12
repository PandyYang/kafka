package com.pandy.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author: Pandy
 * @create: 2022/6/12
 **/
public class CustomerProducerCallbackPartition {

    public static void main(String[] args) {

        // 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,node1:9092,node2:9092");

        // 自定对应的key和value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 关联自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.pandy.kafka.producer.MyPartitioner");

        // 1.创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 2.发送数据
        for (int i = 0; i < 500; i++) {
            kafkaProducer.send(new ProducerRecord<>("first","hello" + i), new Callback() {

                /**
                 * 发送后的回调函数
                 * @param recordMetadata
                 * @param e
                 */
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("主题" + recordMetadata.topic() + "分区" + recordMetadata.partition());
                }
            });
        }
        // 3 关闭连接
        kafkaProducer.close();
    }
}
