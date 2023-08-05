package org.flinkjob.state_stateful;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedWeatherExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableClosureCleaner();

        DataStream<Weather> dataStream = environment.addSource(new WeatherSource());

        dataStream.keyBy("city").print();

        environment.execute();
    }
}
