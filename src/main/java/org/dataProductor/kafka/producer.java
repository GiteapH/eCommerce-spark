package org.dataProductor.kafka;

import java.util.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.dataProductor.productor.mallActDataProductor;
import org.dataProductor.utils.mallActProductorUtils;

/**
 * @author 吕杨平
 * @version 1.0
 * @description: Kafka生产者
 * @date 2023/7/23 19:56
 */
public class producer {

    public static String topic = "test_20210816_2";//定义主题
    public static mallActDataProductor mallActDataProductor= new mallActDataProductor(3,2,1,1,1,1,1,169);
    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");//kafka地址，多个地址用逗号分割
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);

        try {
            while (true) {
                String rows = mallActDataProductor.run(getStartTime(),true);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, rows);
                kafkaProducer.send(record);
                System.out.println("消息发送成功:" + rows.split("\n").length);
                Thread.sleep(1000L * 20);
            }
        } finally {
            kafkaProducer.close();
        }

    }

    private static Date getStartTime(){
        Calendar calendar=Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DATE,-mallActProductorUtils.getDateDiff());

        return calendar.getTime();
    }
}
