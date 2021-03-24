package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Date:2021/3/16
 * Description:
 */
public class MyKafkaUtil {

    //准备配置信息
    private static Properties properties = new Properties();

    //指定默认的主题
    private static final String DWD_DEFAULT_TOPIC = "dwd_default_topic";

    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
    }


    public static FlinkKafkaProducer<String> getKafkaSink(String topic){

        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                properties);
    }

    public static <T>FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSchema){

        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000L+"");
        return new FlinkKafkaProducer<T>(
                DWD_DEFAULT_TOPIC,
                kafkaSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }


    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId,String topic){

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
                );
    }
}
