package com.zqykj.streaming.datepattern;

import java.io.Serializable;

/**
 * Created by weifeng on 2018/4/25.
 */
public class DatePattern implements Serializable {

    private String datetime;
    private String date;
    private String time;

    public String getDatetime() {
        return datetime;
    }

    public void setDatetime(String datetime) {
        this.datetime = datetime;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
