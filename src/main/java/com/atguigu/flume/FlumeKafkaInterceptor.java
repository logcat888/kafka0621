package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @author chenhuiup
 * @create 2020-08-29 15:14
 * //flume拦截器
 */
public class FlumeKafkaInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    /**
     * 处理逻辑:
     *    判断event的body是否包含
     *     "atguigu" --> 在header中设置  topic=atguigu
     *     "shangguigu"-> 在header中设置 topic=shangguigu
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //获取body
        String body = new String(event.getBody());
        //获取headers
        Map<java.lang.String, java.lang.String> headers = event.getHeaders();
        if (body.contains("atguigu")){
            headers.put("topic","atguigu");
        }else if (body.contains("shangguigu")){
            headers.put("topic","shangguigu");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new FlumeKafkaInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
