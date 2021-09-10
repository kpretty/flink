package day04;

import bean.WaterSensor;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取端口数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 1111);

        // 处理数据
        SingleOutputStreamOperator<String> mapDS = socketDS.map((MapFunction<String, String>) value -> {
            String[] els = value.split(" ");
            return JSON.toJSONString(new WaterSensor(els[0], Long.parseLong(els[1]), Double.parseDouble(els[2])));
        }).returns(Types.STRING);

        // 将数据写入 Kafka
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.225:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        mapDS.addSink(new FlinkKafkaProducer<String>("test", new SimpleStringSchema(), properties));

        env.execute();

    }
}
