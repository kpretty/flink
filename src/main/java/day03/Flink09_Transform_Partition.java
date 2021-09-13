package day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink09_Transform_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(4);

        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 1111);

        SingleOutputStreamOperator<String> DS = socketDS.map(x -> x);

        DS.forward().print("forward");
        DS.global().print("global");
        DS.broadcast().print("broadcast");
        DS.keyBy(x -> x).print("keyBy");
        DS.print("map").setParallelism(2);
        DS.shuffle().print("shuffle");
        DS.rebalance().print("rebalance");
        DS.rescale().print("rescale");

        env.execute();
    }
}
