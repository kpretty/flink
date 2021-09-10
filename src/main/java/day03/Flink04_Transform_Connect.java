package day03;


import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink04_Transform_Connect {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从端口创建两条流
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 1111);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 2222);

        ConnectedStreams<String, Integer> connect = source1.connect(source2.map(Integer::parseInt));

        connect.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value;
            }
        }).print();

        env.execute();
    }
}
