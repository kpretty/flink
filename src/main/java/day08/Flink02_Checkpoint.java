package day08;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_Checkpoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置5s 进行一次 checkpoint
        env.enableCheckpointing(5000);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://localhost:8020/flink/ck"));
        // 设置 exactly_once 语义，即 barrier 对齐
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置 30 s 必须完成 checkpoint
        env.getCheckpointConfig().setCheckpointTimeout(30000);
        // 设置 job 终止后保留 checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 任务逻辑
        env.socketTextStream("localhost", 1111)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(x -> x.f0)
                .sum(1)
                .print();

        // 任务执行
        env.execute();
    }
}
