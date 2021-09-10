package day02;

import bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;


public class Flink04_Source_MySource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MySource("localhost", 1111))
                .print();

        env.execute();
    }

    //自定义从端口读取数据
    public static class MySource implements SourceFunction<WaterSensor> {
        // 定义属性
        private String host;
        private int port;
        private Boolean running = true;
        private Socket socket;
        private BufferedReader reader;

        MySource() {
        }

        public MySource(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            socket = new Socket(host, port);

            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            String line = reader.readLine();
            while (running && line != null) {

                String[] els = line.split(" ");
                ctx.collect(new WaterSensor(els[0], Long.parseLong(els[1]), Double.parseDouble(els[2])));
                line = reader.readLine();
            }
        }

        @Override
        public void cancel() {
            // 正常关闭
            running = false;
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
