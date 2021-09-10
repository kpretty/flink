package day04;

import bean.WaterSensor;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Collections;
import java.util.List;

public class Flink03_Sink_ES {
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

        // 将数据写入 ES
        List<HttpHost> hosts = Collections.singletonList(new HttpHost("localhost", 9200));

        ElasticsearchSink.Builder<WaterSensor> elasticsearchSink = new ElasticsearchSink.Builder<>(hosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                IndexRequest request = Requests
                        .indexRequest("sensor")
                        .id(element.getId())
                        .source(JSON.toJSONString(element), XContentType.JSON);
                indexer.add(request);
            }
        });

        elasticsearchSink.setBulkFlushMaxActions(1);


        mapDS.addSink(elasticsearchSink.build());

        env.execute();
    }
}
