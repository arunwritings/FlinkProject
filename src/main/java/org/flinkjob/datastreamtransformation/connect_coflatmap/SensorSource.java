package org.flinkjob.datastreamtransformation.connect_coflatmap;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;
    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        String[] sensorId = new String[2];
        for (int i=0;i<2;i++){
            sensorId[i]="sensor_"+i;
        }

        while (running){
            for (int i=0;i<2;i++){
                Random random = new Random();
                int low = 10;
                int high = 100;
                int result = random.nextInt(high-low)+low;
                sourceContext.collect(new SensorReading(sensorId[i],result));
            }
            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        this.running=false;
    }
}
