package day04;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink05_Sink_Custom {
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

        // 自定义 Sink 写入 Mysql 中
        mapDS.addSink(new MySQLRichSinkFunction("jdbc:mysql://xxx:3306", "xxx", "xxx"));

        env.execute();
    }

    private static class MySQLRichSinkFunction extends RichSinkFunction<WaterSensor> {
        private final String url;
        private final String userName;
        private final String password;
        private Connection con;
        private PreparedStatement ps;

        public MySQLRichSinkFunction(String url, String userName, String password) {
            this.url = url;
            this.userName = userName;
            this.password = password;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            con = DriverManager.getConnection(url, userName, password);
            ps = con.prepareStatement("insert into encryption.sensor(id,ts,vc) values(?,?,?)");
        }

        @Override
        public void close() throws Exception {
            if (ps != null) {
                ps.close();
            }
            if (con != null) {
                con.close();
            }
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setDouble(3, value.getVc());
            ps.execute();
        }
    }
}
