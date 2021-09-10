package day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Project: flink<br/>
 * Package: day02<br/>
 * Version: 1.0<br/>
 * Author: wjun<br/>
 * Description:
 * <br/>
 * Created by hc on 2021/09/08 18:23<br/>
 * © 1996 - 2021 Zhejiang Hong Cheng Computer Systems Co., Ltd.
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.2.225:9092");
        properties.setProperty("group.id", "Flink03_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));

        kafkaDS.print();

        env.execute();

    }
}