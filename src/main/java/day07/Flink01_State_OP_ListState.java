package day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class Flink01_State_OP_ListState {
    private static Long num;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(500);

        env.socketTextStream("localhost", 1111)
                .map(new MyMapFunction())
                .print();
        env.execute();

        System.out.println(num);
    }

    private static class MyMapFunction implements MapFunction<String, String>, CheckpointedFunction {

        private ListState<String> listState;

        @Override
        public String map(String value) throws Exception {
            listState.add(value);
            return value;
        }

        // checkpoint 时调用，可以实现 state 持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            for (String ignored : listState.get()) {
                num++;
            }
        }

        // 每个算子子任务初始化调用一次，初始化状态
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("初始化...");
            listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<String>("num", String.class));
        }
    }
}
