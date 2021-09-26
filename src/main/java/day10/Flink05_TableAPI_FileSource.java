package day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


public class Flink05_TableAPI_FileSource {
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
                "'path' = 'hdfs://localhost:8020/flink/table'," +
                "'format' = 'csv'" +
                ")");

        // 读取临时表
        Table table = tableEnv.from("t_water_sensor");

        // 查询
        tableEnv.toAppendStream(table, Row.class).print();

        // 执行任务
        env.execute();

    }
}
