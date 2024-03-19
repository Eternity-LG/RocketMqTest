package cn.lghonor.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
 
import java.util.List;
 
public class PushConsumer {
 
    public static void consumer() {
        //创建消费者对象
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("TestGroup");
        try {
            //设置NameServer节点
            consumer.setNamesrvAddr("http://43.138.71.194:9876");
            /*订阅主题，
            consumer.subscribe包含两个参数：
            topic: 说明消费者从Broker订阅哪一个主题，这一项要与Provider保持一致。
            subExpression: 子表达式用于筛选tags。
            同一个主题下可以包含很多不同的tags，subExpression用于筛选符合条件的tags进行接收。
            例如：设置为*，则代表接收所有tags数据。
            例如：设置为PAY_TAG，则Broker中只有tags=PAY_TAG的消息会被接收，而其他的就会被排除在外。
            */
            consumer.subscribe("Test", "*");
            //创建监听，当有新的消息监听程序会及时捕捉并加以处理。
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                //批量数据处理
                for (MessageExt msg : msgs) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    System.out.println("消费者获取数据:" + msg.getMsgId() + "==>" + new
                            String(msg.getBody()));
                }
                //返回数据已接收标识
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            //启动消费者，与Broker建立长连接，开始监听。
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 
    public static void main(String[] args) {
        PushConsumer.consumer();
    }
}