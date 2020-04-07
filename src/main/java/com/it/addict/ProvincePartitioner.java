package com.it.addict;

import com.it.love.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean>{
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        int num = Integer.parseInt(key.toString().substring(2, 3));
        if((num-4)<=4 && 0<=(num-4)) return num-4;
        else return 5;
    }
}
