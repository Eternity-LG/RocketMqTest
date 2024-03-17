package cn.lghonor.producer;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class Producer {
 
    public static void producer() {
        //创建DefaultMQProducer消息生产者对象
        DefaultMQProducer producer = new DefaultMQProducer("TestGroup");
        //设置NameServer 多个节点间用分号分割
        producer.setNamesrvAddr("43.138.71.194:9876");
        try {
            //与NameServer建立长连接
            producer.start();
            //发送一百条数据
            for (int i = 1; i <= 300; i++) {
                //1S中发送一次
                Thread.sleep(1000);
                JSONObject json = new JSONObject();
                json.put("orderId",i+1);
                json.put("desc","这是第"+i+"个订单");
                //数据正文
                String data = json.toJSONString();
                /*创建消息
                Message消息三个参数
                topic 代表消息主题，自定义自定义TopicOrder代表订单主题代表订单主题
                tags 代表标志，用于消费者接收数据时进行数据筛选。PAY_TAG代表支付相关信息
                body 代表消息内容
                */
                Message message = new Message("Test", "PAY_TAG", data.getBytes());
                //发送消息，获取发送结果
                SendResult result = producer.send(message);
                //将发送结果对象打印在控制台
                System.out.println("消息已发送：MsgId:" + result.getMsgId() + "，发送状态:"
                        + result.getSendStatus());
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                producer.shutdown();
            } catch (Exception e) {
            }
        }
    }
 
    public static void main(String[] args) {
        Producer.producer();;
    }
}