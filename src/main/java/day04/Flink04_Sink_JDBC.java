package day04;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink04_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取端口数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 1111);

        // 处理数据
        SingleOutputStreamOperator<WaterSensor> mapDS = socketDS.map((MapFunction<String, WaterSensor>) value -> {
            String[] els = value.split(" ");
            return new WaterSensor(els[0], Long.parseLong(els[1]), Double.parseDouble(els[2]));
        }).returns(Types.POJO(WaterSensor.class));

        // 将数据写入 MySQL 中
        String sql = "insert into encryption.sensor(id,ts,vc) values(?,?,?)";

        mapDS.addSink(JdbcSink.sink(sql,
                (ps, waterSensor) -> {
                    ps.setString(1, waterSensor.getId());
                    ps.setLong(2, waterSensor.getTs());
                    ps.setDouble(3, waterSensor.getVc());
                }, JdbcExecutionOptions.builder().withBatchSize(1).build()
                , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://rm-bp11rb5719vqrplkipo.mysql.rds.aliyuncs.com:3306")
                        .withUsername("xxy")
                        .withPassword("Xxy1998_")
                        .build()));

        env.execute();
    }
}
