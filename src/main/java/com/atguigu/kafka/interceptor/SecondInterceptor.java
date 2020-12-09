package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-08-29 14:54
 * 记录消息发送成功和失败的条数
 */
public class SecondInterceptor implements ProducerInterceptor<String, String> {
    private int success;
    private int fail;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord; //注意不要忘了把对象返回
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            success++;
        } else {
            fail++;
        }

    }

    //收尾工作
    @Override
    public void close() {
        System.out.printf("success%d",success);
        System.out.printf("fail%d",fail);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
