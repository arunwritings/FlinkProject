package org.example.flinkjob.collectionbased;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CollectionBasedExample {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.getConfig().disableClosureCleaner();
//        List<String> listObj = new ArrayList<>();
//        listObj.add("one");
//        listObj.add("two");
//        listObj.add("three");
//        DataStream<String> dataStream = streamExecutionEnvironment.fromCollection(listObj);
//        dataStream.print();
            DataStream<String> ds = streamExecutionEnvironment.fromElements("one","two","three");
            ds.print();

            DataStream<Integer> dsInt = streamExecutionEnvironment.fromElements(1,2,3);
            dsInt.print();
        streamExecutionEnvironment.execute();
    }
}
