package org.flinkjob.window;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class TriggerWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableClosureCleaner();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flinkreadkafka");

        DataStream<String> dataStream = environment.addSource(new FlinkKafkaConsumer("tinput", new SimpleStringSchema(), properties));

        dataStream.print();

        dataStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(60))).trigger(CountTrigger.of(3)).apply(new AllWindowFunction<String, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
                int sum=0;
                for (String s : iterable){
                    sum = sum +1;
                }
                collector.collect("Start::"+timeWindow.getStart()+" end::"+timeWindow.getEnd()+" sum::"+sum);
            }
        }).print();
        environment.execute();
    }

    }
