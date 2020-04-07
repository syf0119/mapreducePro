package com.it.fall;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,Text,OrderBean> {
    OrderBean orderBean=new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tuple = value.toString().split("\t");
        orderBean.setOid(tuple[0]);
        orderBean.setPid(tuple[1]);
        orderBean.setMoney(Double.parseDouble(tuple[2]));
        context.write(new Text(tuple[0]),orderBean);
    }
}
