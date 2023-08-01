package org.flinkjob.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

class StringToInteger implements MapFunction<String,Integer>{
    @Override
    public Integer map(String s) throws Exception {
        return Integer.valueOf(s);
    }
}
public class CountWindow {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableClosureCleaner();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id", "flinkreadkafka");

        DataStream<String> dataStream = environment.addSource(new FlinkKafkaConsumer("tinput", new SimpleStringSchema(),properties));
        dataStream.print();

        dataStream.map(new StringToInteger()).countWindowAll(5).sum(0).print();

        environment.execute();

    }
}
