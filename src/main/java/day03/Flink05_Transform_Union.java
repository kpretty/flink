package day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Flink05_Transform_Union {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从多个源创建多条流
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 1111);
        DataStreamSource<String> source2 = env.readTextFile("input");
        DataStreamSource<String> source3 = env.fromCollection(Arrays.asList("java scala", "scala kafka", "python R"));

        DataStream<String> unionDS = source1.union(source2, source3);

        unionDS.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .sum(1)
                .print();

        env.execute();

    }
}
