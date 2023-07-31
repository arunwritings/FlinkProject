package org.flinkjob.datastreamtransformation.connect_coflatmap;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SmokeLevelSource implements SourceFunction<SmokeLevel> {

    private boolean running=true;
    @Override
    public void run(SourceContext<SmokeLevel> sourceContext) throws Exception {
        Random random = new Random();
        while (running){
            if (random.nextGaussian()>0.8){
                sourceContext.collect(SmokeLevel.HIGH);
            }else {
                sourceContext.collect(SmokeLevel.LOW);
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.running=false;
    }
}
