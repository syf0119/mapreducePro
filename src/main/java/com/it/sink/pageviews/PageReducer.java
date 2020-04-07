package com.it.sink.pageviews;

import com.it.sink.preprocess.ParseBean;
import com.it.sink.preprocess.WebLogBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class PageReducer extends Reducer<Text, WebLogBean, PageViewsBean, NullWritable> {
    PageViewsBean pageViewsBean = new PageViewsBean();
    @Override
    protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
        List<WebLogBean> list = new ArrayList<>();
        String sess = UUID.randomUUID().toString();
        int step = 0;
        for (WebLogBean value : values) {
            WebLogBean webLogBean = new WebLogBean();
            try {
                BeanUtils.copyProperties(webLogBean, value);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            list.add(webLogBean);
        }
        Collections.sort(list, new WebLogBeanComparator());
        if (list.size() == 1) {
            WebLogBean webLogBean = list.get(0);
            setProperties(pageViewsBean, webLogBean,UUID.randomUUID().toString(),1,"60");
            context.write(pageViewsBean, NullWritable.get());
        }
        if (list.size() >1) {
            for (int x = 1; x < list.size(); x++) {

                WebLogBean next = list.get(x);
                WebLogBean pre = list.get(x - 1);


               // System.out.println(pre.getRemote_addr()+"   "+pre.getTime_local());
                step++;
                try {
                    long nextTime = (ParseBean.getSdf().parse(next.getTime_local()).getTime()) / 1000;
                    long preTime = (ParseBean.getSdf().parse(pre.getTime_local()).getTime()) / 1000;

                    if (nextTime - preTime < 1800) {

                        long stayTime = nextTime - preTime;
                        //System.out.println(stayTime);
                        pageViewsBean.setSession(sess);
                        pageViewsBean.setStep(step);
                        pageViewsBean.setStaylong(Long.toString(stayTime));

                        context.write(pageViewsBean, NullWritable.get());
                    } else {
                        setProperties(pageViewsBean, pre,sess,step,"60");
                        step = 0;
                        sess= UUID.randomUUID().toString();
                        context.write(pageViewsBean, NullWritable.get());
                    }
                    if(x==(list.size()-1)){
                        setProperties(pageViewsBean,next,sess,step,"60");
                        context.write(pageViewsBean, NullWritable.get());
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void setProperties(PageViewsBean pageViewsBean, WebLogBean pre,String sess,int step,String stayLong) {
        pageViewsBean.setBytes_send(pre.getBody_bytes_sent());
        pageViewsBean.setReferal(pre.getHttp_referer());
        pageViewsBean.setRemote_addr(pre.getRemote_addr());
        pageViewsBean.setRequest(pre.getRequest());
        pageViewsBean.setUseragent(pre.getHttp_user_agent());
        pageViewsBean.setTimestr(pre.getTime_local());
        pageViewsBean.setStatus(pre.getStatus());
        pageViewsBean.setStaylong(stayLong);
        pageViewsBean.setStep(step);
        pageViewsBean.setSession(sess);
    }
}

