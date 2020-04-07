package com.it.love;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    @Override
    protected void map(LongWritable  key, Text value, Context context) throws IOException, InterruptedException {
        if(!value.toString().trim().isEmpty()) {
            String[] strs = value.toString().split("\\t");
//            System.out.println("*****************");
//            System.out.println(Arrays.toString(strs));
//            System.out.println("*******************");
            FlowBean flowBean = new FlowBean();
            flowBean.setUpFlow(Long.parseLong(strs[strs.length - 3]));
            flowBean.setDownFlow(Long.parseLong(strs[strs.length - 2]));
            context.write(new Text(strs[0]), flowBean);
        }
    }
}
