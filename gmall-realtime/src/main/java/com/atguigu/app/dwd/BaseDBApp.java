package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSink;
import com.atguigu.app.func.MyDeserializationSchemaFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.Nullable;

/**
 * Date:2021/3/17
 * 数据流：Web/App>>Nginx>>SpringBoot>>MySQL>>Flink(FlinkCDCApp)>>Kafka(ods)>>Flink(BaseDBApp)>>Kafka/Phoenix
 * 框架服务：Mock(Mock>>Nginx>>SpringBoot)>>MySQL>>FlinkCDCApp>>Kafka(ZK)>>BaseDBAPP>>Kafka/Phoenix
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置应该与Kafka主题的分区数一致
        env.setParallelism(1);
/*
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
        System.setProperty("HADOOP_USER_NAME", "atguigu");
*/

        //TODO 2 读取Kafka ods_base_db 主题数据创建流

        String groupId = "base_db_app_group";
        String topic = "ods_base_db";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(groupId, topic));

        //TODO 3 过滤空值数据(主流)
        //过滤不完整Json数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("发现脏数据" + value);
                }
            }
        });
        //过滤Json字符串中，data部分为空值的数据，空值数据没有意义
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(jsonObj -> {
            String data = jsonObj.getString("data");
            return data != null && data.length() > 0;
        });

        //TODO 4 使用FlinkCDC读取配置表形成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021-realtime")
                .tableList("gmall2021-realtime.table_process")
                .deserializer(new MyDeserializationSchemaFunction())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table_process", String.class, TableProcess.class);
        BroadcastStream<String> tableProcessBS = mysqlDS.broadcast(mapStateDescriptor);

        //TODO 5 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(tableProcessBS);

        //TODO 6 分流
        OutputTag<JSONObject> hbaseOutputTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> resultDS = connectedStream.process(new TableProcessFunction(hbaseOutputTag, mapStateDescriptor));

        //TODO 7 将分好的流写入Phoenix表(维度数据)或者Kafka主题(事实数据)
        filterDS.print("原始数据>>>");
        resultDS.print("Kafka>>>");
        resultDS.getSideOutput(hbaseOutputTag).print("HBase>>>");

        //将数据写入Phoenix
        resultDS.getSideOutput(hbaseOutputTag).addSink(new DimSink());

        //将数据写入Kafka
        resultDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>(){
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化Kafka数据");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(
                        jsonObject.getString("sinkTable"),
                        jsonObject.getString("data").getBytes()
                );
            }
        }));

        //TODO 8 启动任务
        env.execute();
    }
}
