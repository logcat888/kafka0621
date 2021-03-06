package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产者
 * <p>
 * Kafka提供的配置类:
 * ProducerConfig  生产者相关的配置项
 * ConsumerConfig  消费者相关的配置项
 * CommonClientConfigs : 通用的配置项
 *
 * @author chenhuiup
 * @create 2020-08-29 8:21
 */
public class ProducerDemo {
    public static void main(String[] args) {
        //0.设置配置项
        Properties properties = new Properties();
        //kafka集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //ack级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, "5");
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        //等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        //RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        //指定key和value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //1.获取生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2.生产消息
        for (int i = 0; i < 10; i++) {
            //粘性分区
//            kafkaProducer.send(new ProducerRecord<>("atguigu", "message^^^^^^" + i));
            //指定分区号
//            kafkaProducer.send(new ProducerRecord<>("atguigu", 1, null, "message-->" + i));

            //指定key
            kafkaProducer.send(new ProducerRecord<>("atguigu", "key" + i, "message=====" + i));
        }

        //3.关闭生产者对象
        kafkaProducer.close();
    }
}
