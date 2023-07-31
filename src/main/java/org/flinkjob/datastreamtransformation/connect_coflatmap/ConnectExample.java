package org.flinkjob.datastreamtransformation.connect_coflatmap;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

class RaisedAlertFlatMap implements CoFlatMapFunction<SensorReading,SmokeLevel,Alert>{

    private SmokeLevel smokeLevel = SmokeLevel.LOW;
    @Override
    public void flatMap1(SensorReading arg0, Collector<Alert> arg1) throws Exception {
        if (this.smokeLevel == SmokeLevel.HIGH && arg0.temperature>80){
            arg1.collect(new Alert("Risk of Fire! "+arg0, arg0.temperature));
        }
    }

    @Override
    public void flatMap2(SmokeLevel arg0, Collector<Alert> arg1) throws Exception {
        this.smokeLevel=arg0;
    }
}
public class ConnectExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();

        DataStream<SmokeLevel> smokeLevelDataStream = streamExecutionEnvironment.addSource(new SmokeLevelSource());

        DataStream<SensorReading> sensorReadingDataStream = streamExecutionEnvironment.addSource(new SensorSource());

        KeyedStream<SensorReading,String> keyedStream = sensorReadingDataStream.keyBy(r-> r.id);
        keyedStream.connect(smokeLevelDataStream).flatMap(new RaisedAlertFlatMap()).print();

        streamExecutionEnvironment.execute();
    }
}
