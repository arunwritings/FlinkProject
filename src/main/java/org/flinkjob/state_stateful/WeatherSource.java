package org.flinkjob.state_stateful;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class WeatherSource extends RichParallelSourceFunction<Weather> {

    @Override
    public void run(SourceContext<Weather> sourceContext) throws Exception {
        List<String> cityList = new ArrayList<>();
        cityList.add("Bengaluru");
        cityList.add("Mysuru");
        cityList.add("Kolara");

        while (true){
            for (String city : cityList){
                Integer randomTemp = ThreadLocalRandom.current().nextInt(1,20+1);
                sourceContext.collect(new Weather(city, randomTemp));
            }
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {

    }
}
