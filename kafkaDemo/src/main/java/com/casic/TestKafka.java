package com.casic;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.*;

public class TestKafka {


    /**
     * 测试生产者
     */
    @Test
    public void testProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "s202:9092");
        props.put("acks", "all");
       // props.put("retries", 0);
       // props.put("batch.size", 16384);
       // props.put("linger.ms", 1);
      //  props.put("buffer.memory", 33554432);
            //key的串行化类型
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            //value的串行化类型
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
       //创建一个kafka生产者
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        /**
         * 参数2:指定那个分区
         */

        producer.send(new ProducerRecord<String, String>("test0",2,"1000","tomcat" ));
        //关闭生产者
        producer.close();
    }

    @Test
    public void testCustomer1(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "s202:9092");
        props.put("zookeeper.connect", "s202:2181");
        props.put("group.id", "g1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
       // consumer.seekToBeginning(Arrays.asList(new TopicPartition("test0",0)));
       consumer.subscribe(Arrays.asList("test0"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            //consumer.close();
        }
    }

    @Test
    public void testCustomer(){
        Properties props = new Properties();
        props.put("zookeeper.connect", "s202:2181");
        props.put("group.id", "g3");
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //创建消费者配置对象
        ConsumerConfig config = new ConsumerConfig(props);
        //
        Map<String, Integer> map = new HashMap<String, Integer>();
        //设置topic
        map.put("test0", new Integer(1));
       //获取Kafka中的消息
        Map<String, List<KafkaStream<byte[], byte[]>>> msgs = Consumer.createJavaConsumerConnector(new ConsumerConfig(props)).createMessageStreams(map);
        //根据topic来获取数据
        List<KafkaStream<byte[], byte[]>> msgList = msgs.get("test0");
        //循环
        for(KafkaStream<byte[],byte[]> stream : msgList){
            ConsumerIterator<byte[],byte[]> it = stream.iterator();
            while(it.hasNext()){
                byte[] message = it.next().message();
                System.out.println(new String(message));
            }
        }
    }

}
