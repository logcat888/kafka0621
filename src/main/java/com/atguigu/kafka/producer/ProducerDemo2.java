package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 *  * Kafka提供的配置类:
 *  *    ProducerConfig  生产者相关的配置项
 *  *    ConsumerConfig  消费者相关的配置项
 *  *    CommonClientConfigs : 通用的配置项
 *
 *  回调函数会在producer收到ack时调用，为异步调用，该方法有两个参数，
 *  分别是RecordMetadata和Exception，如果Exception为null，说明消息发送成功，
 *  如果Exception不为null，说明消息发送失败。
 * @author chenhuiup
 * @create 2020-08-29 8:22
 */
public class ProducerDemo2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //0.设置配置项
        Properties properties = new Properties();
        //kafka集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //ack级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, "5");
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");//16K
        //等待时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        //RecordAccumulator缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");//32M
        //指定key和value的序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //1.获取生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2.生产消息
        for (int i = 0; i < 10; i++) {
            //粘性分区
            kafkaProducer.send(new ProducerRecord<>("atguigu", "message^^^^^^" + i),
                    /**
                     * 消息发送完成后，成功或者出现异常，都要回来调用onCompletion方法.
                     * @param metadata  消息成功，会存储消息的元数据信息。
                     * @param exception 消息失败, 相应的异常会封装到exception中。
                     *                  注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试。
                     */
                    (metadata,e)->{
                if (e == null){//发送成功
                    System.out.println("topic:" + metadata.topic() + ",partition:" + metadata.partition() + ",offset:" + metadata.offset());
                }else {//发送失败
                    System.out.println(e.getMessage());
                }
            }).get();//send方法会放回Future对象，Future的get方法会线程阻塞，直到回调方法返回信息，主线程才继续执行

            System.out.printf("第%d次发送\n",i);

        }

        //3.关闭生产者对象
        kafkaProducer.close(); //关闭生产者对象时，会强制将RecordAccumulator中batch的消息发送出去，不需要考虑batch发送消息的条件16k和发送时间
    }
}
