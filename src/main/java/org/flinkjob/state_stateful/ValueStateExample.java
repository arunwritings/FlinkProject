package org.flinkjob.state_stateful;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

class MaxTempValueState extends RichFlatMapFunction<Weather, Tuple2<String, Integer>>{

    private ValueState<Integer> maxTemp;
    @Override
    public void flatMap(Weather weather, Collector<Tuple2<String, Integer>> collector) throws Exception {
        Integer currentMax = maxTemp.value();
        if (currentMax == null){
            maxTemp.update(weather.temperature);
        } else {
            if (currentMax< weather.temperature){
                System.out.println("Previous State: "+maxTemp.value());
                maxTemp.update(weather.temperature);
                collector.collect(new Tuple2<>(weather.city, weather.temperature));
                System.out.println("Latest state: "+maxTemp.value());
            }
            
        }
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("MaxTemp", TypeInformation.of(new TypeHint<Integer>() {}));
        maxTemp = getRuntimeContext().getState(valueStateDescriptor);
    }
}
public class ValueStateExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().disableClosureCleaner();

        DataStream<Weather> dataStream = environment.addSource(new WeatherSource());
        dataStream.keyBy("city").flatMap(new MaxTempValueState()).print();

        environment.execute();
    }
}
