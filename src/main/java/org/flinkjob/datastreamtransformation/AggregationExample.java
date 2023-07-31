package org.flinkjob.datastreamtransformation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();

        Tuple2<String,Integer> p1 = new Tuple2<>("Kohli",50);
        Tuple2<String,Integer> p2 = new Tuple2<>("Dhoni",75);
        Tuple2<String,Integer> p3 = new Tuple2<>("Rohit",100);
        Tuple2<String,Integer> p4 = new Tuple2<>("Kohli",100);
        Tuple2<String,Integer> p5 = new Tuple2<>("Dhoni",100);
        Tuple2<String,Integer> p6 = new Tuple2<>("Kohli",75);

        DataStream<Tuple2<String,Integer>> dataStream =streamExecutionEnvironment.fromElements(p1,p2,p3,p4,p5,p6);

        KeyedStream<Tuple2<String,Integer>, String> key_ds = dataStream.keyBy(v-> v.f0);

        //key_ds.max(1).print();

        //key_ds.min(1).print();

        key_ds.sum(1).print();

        streamExecutionEnvironment.execute();
    }
}
