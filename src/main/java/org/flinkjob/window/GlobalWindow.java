package org.flinkjob.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

class StringToTuple2 implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String arg0) throws Exception {
        String[] arr = arg0.split(",");
        return new Tuple2(arr[0], Integer.valueOf(arr[1]));
    }
}

public class GlobalWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer("tinput", new SimpleStringSchema(), properties));
        stream.print();

        stream.map(new StringToTuple2()).windowAll(GlobalWindows.create()).trigger(CountTrigger.of(3)).sum(1).print();
        env.execute();
    }

}
