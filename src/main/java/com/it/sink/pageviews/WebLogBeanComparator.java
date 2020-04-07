package com.it.sink.pageviews;

import com.it.sink.preprocess.WebLogBean;

import java.util.Comparator;

public class WebLogBeanComparator  implements Comparator<WebLogBean>{

    @Override
    public int compare(WebLogBean o1, WebLogBean o2) {
        String time1 = o1.getTime_local().trim();
        String time2 = o2.getTime_local().trim();
        return time1.compareTo(time2);

    }
}
