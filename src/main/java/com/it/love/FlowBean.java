package com.it.love;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable{
    private Long upFlow;
    private Long downFlow;


    public Long getTotalFlow() {
        return upFlow+downFlow;
    }



    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }



    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +

                '}';
    }
    @Override
    public void write(DataOutput out) throws IOException {
       out.writeLong(upFlow);
       out.writeLong(downFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow=in.readLong();
        this.downFlow=in.readLong();

    }
}
