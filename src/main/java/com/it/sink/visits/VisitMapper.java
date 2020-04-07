package com.it.sink.visits;

import com.it.sink.pageviews.PageViewsBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VisitMapper extends Mapper<LongWritable,Text,Text,PageViewsBean> {
    PageViewsBean pageViewsBean = new PageViewsBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tuple = value.toString().split("\001");
        Text text = new Text(tuple[1]);
        for (int x = 0; x < tuple.length; x++) {

            pageViewsBean.setRemote_addr(tuple[0]);
            pageViewsBean.setSession(tuple[1]);
            pageViewsBean.setTimestr(tuple[2]);
            pageViewsBean.setStep(Integer.parseInt(tuple[3]));
            pageViewsBean.setStaylong(tuple[4]);
            pageViewsBean.setBytes_send(tuple[5]);
            pageViewsBean.setReferal(tuple[6]);
            pageViewsBean.setRequest(tuple[7]);
            pageViewsBean.setUseragent(tuple[8]);
            pageViewsBean.setStatus(tuple[9]);


        }
        context.write(text,pageViewsBean);
    }
}
