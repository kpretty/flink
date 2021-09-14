package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


public class Flink05_WaterMark_Periodic {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置定时生成时间
        env.getConfig().setAutoWatermarkInterval(200);

        // 设置并行度，不设置涉及 watermark 传递
        env.setParallelism(1);

        // 读取数据，转换成 POJO
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 1111)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] els = value.split(" ");
                        return new WaterSensor(els[0], Long.parseLong(els[1]), Double.parseDouble(els[2]));
                    }
                }).returns(Types.POJO(WaterSensor.class));

        // 提取事件时间
        WatermarkStrategy<WaterSensor> wm = new WatermarkStrategy<WaterSensor>() {

            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriodic(2000);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000;
            }
        });

        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS
                // 注册事件时间
                .assignTimestampsAndWatermarks(wm)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 允许数据迟到 2s
                .allowedLateness(Time.seconds(2))
                // 3s + 2s 后的数据放入测输出流
                .sideOutputLateData(new OutputTag<WaterSensor>("Side") {
                })
                .sum("vc");

        result.print("master");
        result.getSideOutput(new OutputTag<WaterSensor>("Side") {
        }).print("side");

        env.execute();
    }

    private static class MyPeriodic implements WatermarkGenerator<WaterSensor> {
        private long maxTs;
        // 允许延迟的最大时间
        private final long maxDelay;

        MyPeriodic(long maxDelay) {
            this.maxDelay = maxDelay;
            // 因为周期生成，需要在赋值加 maxDelay，否则调动 onPeriodicEmit 直接变成最大值
            maxTs = Long.MIN_VALUE + maxDelay - 1;
        }

        // 每条数据调用一次
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            // 保证 WaterMark 递增
            maxTs = Math.max(maxTs, eventTimestamp);
        }

        // 周期性调用(默认200ms)
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTs - maxDelay - 1));
        }
    }

}
