package com.example.rocketmqdemo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RocketmqdemoApplicationTests {

    @Test
    public void contextLoads() {
    }
    /**
     * 普通消息生产者
     *
     * @throws Exception
     */
    @Test
    public void test1() throws Exception {
        //设置组名,并实例化
        DefaultMQProducer producer = new
                DefaultMQProducer("producerGroup1");
        // 名称服务,分号分割
        producer.setNamesrvAddr("192.168.40.205:9876;192.168.40.206:9876");
        producer.start();
        for (int i = 0; i < 100000; i++) {
            //消息实例
            Message msg = new Message("topic1" /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
    /**
     * 接受
     */
    /**
     * 普通消息消费者
     * 采用的是Consumer Push的方式
     */
    @Test
    public void test2() {
        //实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumerGroup1");
        consumer.setNamesrvAddr("192.168.40.205:9876;192.168.40.206:9876");
        try {
            //订阅topic1下的所有tag的消息
            consumer.subscribe("topic1", "*");
            //注册回调
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
            System.out.printf("Consumer Started.%n");
            while (true) {
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }


}
