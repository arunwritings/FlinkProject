package org.flinkjob.window;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;

public class SlidingWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer("tinput", new SimpleStringSchema(), properties));
        stream.print();
        stream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(10))).apply(new AllWindowFunction<String, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow arg0, Iterable<String> input, Collector<String> output) throws Exception {
                int sum = 0;
                for (String i : input) {
                    System.out.println(i);
                    sum = sum + 1;
                }
                output.collect(new Date(arg0.getStart()) + ":" + new Date(arg0.getEnd())
                        + ":sum" + String.valueOf(sum));
            }
        }).print();
        env.execute();
    }

}
