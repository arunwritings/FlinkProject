package org.flinkjob.sideoutputs;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

class CSVToString extends RichFlatMapFunction<String, String>{
        IntCounter userCount = new IntCounter();
        Histogram salaryHistogram = new Histogram();

    @Override
    public void open(Configuration parameters) throws Exception {
       super.open(parameters);
       getRuntimeContext().addAccumulator("userCount",this.salaryHistogram);
    }
    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        userCount.add(1);
        String arr[] = s.split(",");
        salaryHistogram.add(Integer.valueOf(arr[4]));
    }
}
public class AccumulatorExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = environment.readTextFile("C:\\Flink\\input\\SideOutput.xlsx").flatMap(new CSVToString());

        JobExecutionResult result = dataStream.getExecutionEnvironment().execute();

        System.out.println((String) result.getAccumulatorResult("" + result.getAccumulatorResult("userCount")));;
    }
}
