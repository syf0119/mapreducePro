package com.it.sink.pageviews;

import com.it.sink.pageviews.PageViewsBean;

import java.util.Comparator;

public class PageViewsBeanComparator implements Comparator<PageViewsBean> {
    @Override
    public int compare(PageViewsBean o1, PageViewsBean o2) {
        return o1.getTimestr().compareTo(o2.getTimestr());
    }
}
