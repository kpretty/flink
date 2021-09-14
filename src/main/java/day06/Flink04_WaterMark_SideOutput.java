package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


public class Flink04_WaterMark_SideOutput {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        WatermarkStrategy<WaterSensor> wm = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
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

}
