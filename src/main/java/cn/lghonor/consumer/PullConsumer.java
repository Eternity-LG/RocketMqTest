package cn.lghonor.consumer;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
 
import java.util.List;
 
public class PullConsumer {
    public static volatile boolean running = true;
    public static void consumer() {
        //创建pull消费者对象
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("TestGroup");
        //设置NameServer节点
        litePullConsumer.setNamesrvAddr("43.138.71.194:9876");
        try {
            //订阅主题，与Push相同
            litePullConsumer.subscribe("Test", "*");
            //每次拉取数据条目数
            litePullConsumer.setPullBatchSize(2);
            //启动消费者
            litePullConsumer.start();
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                //批量数据处理
                for (MessageExt msg : messageExts) {
                    System.out.println("消费者获取数据:" + msg.getMsgId() + "==>" + new
                            String(msg.getBody()));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            litePullConsumer.shutdown();
        }
    }
 
    public static void main(String[] args) {
        PullConsumer.consumer();
    }
}