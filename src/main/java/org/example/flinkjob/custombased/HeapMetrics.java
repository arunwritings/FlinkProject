package org.example.flinkjob.custombased;

public class HeapMetrics {

    public String area;

    public Long maxMemory;

    public Long usedMemory;

    public Integer jobId;

    public String hostName;

    public HeapMetrics(){

    }
    public HeapMetrics(String area, Long maxMemory, Long usedMemory, Integer jobId, String hostName){
        this.area=area;
        this.maxMemory=maxMemory;
        this.usedMemory=usedMemory;
        this.jobId=jobId;
        this.hostName=hostName;
    }

    @Override
    public String toString() {
        return "HeapMetrics{" +
                "area='" + area + '\'' +
                ", maxMemory=" + maxMemory +
                ", usedMemory=" + usedMemory +
                ", jobId=" + jobId +
                ", hostName='" + hostName + '\'' +
                '}';
    }
}
