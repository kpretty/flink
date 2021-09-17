package day07;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink08_Case_Timer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<String> result = env.socketTextStream("localhost", 1111)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] els = value.split(" ");
                        return new WaterSensor(els[0], Long.parseLong(els[1]), Double.parseDouble(els[2]));
                    }
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 上一次水位线
                    private ValueState<Double> lastVc;
                    // 定时器定的时间
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Double>("Vc", Double.class));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("Ts", Long.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 如果水位线状态为null 或者 大于这次水位线正常输出
                        if (lastVc.value() == null || lastVc.value() >= value.getVc()) {
                            out.collect(value.toString());
                            // 尝试删除定时器
                            if (timerTs.value() != null) {
                                ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                                System.out.println("定时器" + timerTs.value() + "删除");
                            }
                        } else {
                            // 水位线升高了
                            timerTs.update(System.currentTimeMillis() + 10000);
                            ctx.timerService().registerProcessingTimeTimer(timerTs.value());
                            System.out.println("定时器" + timerTs.value() + "创建");
                        }
                        // 更新水位状态
                        lastVc.update(value.getVc());
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        ctx.output(new OutputTag<String>("告警数据") {
                        }, ctx.getCurrentKey());
                    }
                });

        result.print("正常数据");

        result.getSideOutput(new OutputTag<String>("告警数据") {
        }).print("告警数据");

        env.execute();
    }
}
