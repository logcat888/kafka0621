package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-08-29 14:54
 * 拦截每条消息,在消息内容的前面添加时间戳
 */
public class FirstInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 取出消息的value
        String value = producerRecord.value();
        //处理
        value = System.currentTimeMillis() + value;
        //构造消息
        ProducerRecord<String, String> newRecord = new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), value);
        return newRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
