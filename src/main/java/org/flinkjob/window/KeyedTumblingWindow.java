package org.flinkjob.window;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

class MyTuple2Class implements MapFunction< String, Tuple2<String, Integer> >{

    @Override
    public Tuple2<String,Integer> map(String arg0) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(arg0);

        String uId = jsonNode.get("uId").textValue();
        String score = jsonNode.get("score").textValue();
        return new Tuple2<>(uId,Integer.valueOf(score));
    }
}
public class KeyedTumblingWindow {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableClosureCleaner();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id", "flinkreadkafka");

        DataStream<String> dataStream = environment.addSource(new FlinkKafkaConsumer("tinput", new SimpleStringSchema(),properties));

        dataStream.map(new MyTuple2Class()).keyBy(0).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {

            @Override
            public void apply(Tuple arg0, TimeWindow arg1, Iterable<Tuple2<String, Integer>> arg2, Collector<String> arg3) throws Exception {
                int sum = 0;
                for (Tuple2<String, Integer> i : arg2){
                    sum=sum+1;
                }
                arg3.collect(arg0.toString()+" Start::"+new Date(arg1.getStart())+" End::"+new Date(arg1.getEnd())+" Count::"+sum);
            }
        }).print();
        environment.execute();
    }
}
