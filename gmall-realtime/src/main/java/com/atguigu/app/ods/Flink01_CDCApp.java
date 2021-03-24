package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyDeserializationSchemaFunction;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * Date:2021/3/16
 * Description:
 */
public class Flink01_CDCApp {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //使用DataStream方式的CDC读取MySQL变化数据(只读取最新的数据)
        DebeziumSourceFunction<String> sourceFunction = MySQLSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021")
                .deserializer(new MyDeserializationSchemaFunction())
//                .startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);


        //将数据写入Kafka
        String topic = "ods_base_db";
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(topic);
        mysqlDS.addSink(kafkaSink);
        mysqlDS.print(">>>>>");


        //启动任务
        env.execute();
    }
}
