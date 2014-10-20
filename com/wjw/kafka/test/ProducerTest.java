/**
 * Created by wjw on 14-10-19.
 */
package com.wjw.kafka.test;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerTest {
    public static void main(String[] args) {
        //long events = Long.parseLong(args[0]);
        Random rnd = new Random();

        Properties props = new Properties();
        // props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","192.168.0.8:9092");
//        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
//        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);
        String[] sentences = {
                "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs",
                "i am at two with nature"
        };
//        for (long nEvents = 0; nEvents < 10; nEvents++) {
//            long runtime = new Date().getTime();
//            String ip = "192.168.2." + rnd.nextInt(255);
//            String msg = runtime + ",www.example.com," + ip;
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//            producer.send(data);
//        }
//        producer.close();
        int length = sentences.length;
        int i = 300000;
        while(i > 1) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Random random = new Random();
            String msg = sentences[random.nextInt(length)];
            //long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
            producer.send(data);
            i--;
        }
        producer.close();
    }
}

