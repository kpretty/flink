package day07;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink02_State_OP_BroadCastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> aDS = env.socketTextStream("localhost", 1111);
        DataStreamSource<String> bDS = env.socketTextStream("localhost", 2222);

        // 广播流
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);
        BroadcastStream<String> broadcast = bDS.broadcast(stateDescriptor);

        // 数据流和广播流进行连接
        aDS.connect(broadcast)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                        String state = broadcastState.get("state");
                        if ("1".equals(state)) {
                            out.collect(value);
                        } else if ("2".equals(state)) {
                            out.collect(value.substring(0, 3));
                        } else {
                            System.out.println("错误");
                        }
                    }

                    @Override
                    public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        // 把值放到广播流中
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                        broadcastState.put("state", value);
                    }
                })
                .print();

        env.execute();
    }
}
