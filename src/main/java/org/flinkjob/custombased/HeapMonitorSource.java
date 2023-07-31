package org.flinkjob.custombased;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;

public class HeapMonitorSource extends RichParallelSourceFunction<HeapMetrics> {

    @Override
    public void run(SourceContext<HeapMetrics> arg0) throws Exception {
        while (true){
            Integer jobId = this.getRuntimeContext().getIndexOfThisSubtask();
            String hostName = InetAddress.getLocalHost().getHostName();

            for (MemoryPoolMXBean memoryPoolMXBean: ManagementFactory.getMemoryPoolMXBeans()){
                if (memoryPoolMXBean.getType()== MemoryType.HEAP){
                    MemoryUsage memoryUsage = memoryPoolMXBean.getUsage();
                    Long maxMemory = memoryUsage.getMax();
                    Long usedMemory = memoryUsage.getUsed();
                    arg0.collect(new HeapMetrics(memoryPoolMXBean.getName(), maxMemory, usedMemory, jobId, hostName));
                }
            }
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {

    }
}
