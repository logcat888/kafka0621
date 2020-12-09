package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author chenhuiup
 * @create 2020-08-29 8:23
 */
public class ConsumerDemo {
    public static void main(String[] args) {
        //0.设置配置项
        Properties properties = new Properties();
        //连接kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        //自动提交offset的时间
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);
        //指定消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"fangfang");
//        //设置退避时间，防止长轮循:不知道对不对
//        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG,1000);

        /**
         *  offset重置:
         *     配置项: auto.offset.reset
         *
         *     配置项的值:
         *     earliest: automatically reset the offset to the earliest offset (头)
         *     latest: automatically reset the offset to the latest offset (尾)
         *
         *  什么情况下会重置offset?
         *   1. 新的组  当前消费者组在kafka内部没有任何消费记录.
         *   2. 数据超过7天被删除.  当前消费者所要消费的offset在kafka内部已经被删除.
         *
         *  总结：当消费者手握的GTP（group，topic，offset）与kafka集群记录的offset对应不上时会根据topic 的offset进行重置到最早或最新
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //指定key和value 的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //1.获取kafka消费者对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //2.订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("atguigu");
        topics.add("first");
        kafkaConsumer.subscribe(topics);

        //3.消费数据

        while (true){
            //消费者拉取数据，注意这个一定要放在while里面
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.println("topic:"+consumerRecord.topic()+
                        ",partition:"+ consumerRecord.partition()+
                        ",key:"+ consumerRecord.key() +
                        ",value:" + consumerRecord.value() +
                        ",offset:"+ consumerRecord.offset());
            }
        }
    }
}
