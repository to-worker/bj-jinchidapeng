package com.zqykj.streaming.metadata;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * Created by weifeng on 2018/5/4.
 */
public class BussinessStatistics implements Serializable {

    private String metricName;
    private String taskId;
    private String resId;
    private Long inTotalRecords;
    private Long outTotalRecords;
    private List<ElpTypeStatistics> elpTypeStatistic;
    private Date recordTime;
    private Date startTime;
    private Date endTime;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public List<ElpTypeStatistics> getElpTypeStatistic() {
        return elpTypeStatistic;
    }

    public void setElpTypeStatistic(List<ElpTypeStatistics> elpTypeStatistic) {
        this.elpTypeStatistic = elpTypeStatistic;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getResId() {
        return resId;
    }

    public void setResId(String resId) {
        this.resId = resId;
    }

    public Long getInTotalRecords() {
        return inTotalRecords;
    }

    public void setInTotalRecords(Long inTotalRecords) {
        this.inTotalRecords = inTotalRecords;
    }

    public Long getOutTotalRecords() {
        return outTotalRecords;
    }

    public void setOutTotalRecords(Long outTotalRecords) {
        this.outTotalRecords = outTotalRecords;
    }

    public Date getRecordTime() {
        return recordTime;
    }

    public void setRecordTime(Date recordTime) {
        this.recordTime = recordTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
}
