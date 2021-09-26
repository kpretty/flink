package day10;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class Flink06_TableAPI_FileSink {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建临时表
        tableEnv.executeSql("create table t_water_sensor (" +
                "id string," +
                "ts bigint," +
                "vc double" +
                ") with (" +
                "'connector' = 'filesystem'," +
                "'path' = 'hdfs://localhost:8020/flink/table/ds3'," +
                "'format' = 'csv'" +
                ")");

        // 读取端口数据
        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("localhost", 1111)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] els = value.split(" ");
                        return new WaterSensor(els[0], Long.parseLong(els[1]), Double.parseDouble(els[2]));
                    }
                });

        // 将数据写入表中
        tableEnv.fromDataStream(wsDS)
                .executeInsert("t_water_sensor");

        // 执行任务
        env.execute();

    }
}
