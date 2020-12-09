package com.atguigu.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-08-29 8:24
 * 1）默认的分区器 DefaultPartitioner
 */
public class MyPartitioner implements Partitioner {
    /**
     * 计算某条消息要发送到哪个分区
     * @param s 主题
     * @param o 消息的key
     * @param bytes 消息的key序列化后的字节数组
     * @param o1 消息的value
     * @param bytes1 消息的value序列化后的字节数组
     * @param cluster
     * @return
     *
     * 需求: 以atguigu主题为例，2个分区
     *       消息的 value包含"atguigu"的 进入0号分区
     *       其他的消息进入1号分区
     */
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String s1 = o1.toString();
        int p;
        if (s1.contains("atguigu")){
            p = 0;
        }else {
            p = 1;
        }
        return p;
    }

    /**
     * 收尾工作
     */
    @Override
    public void close() {

    }

    /**
     * 读取配置的
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
