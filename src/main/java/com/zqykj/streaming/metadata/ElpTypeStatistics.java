package com.zqykj.streaming.metadata;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * Created by weifeng on 2018/5/4.
 */
public class ElpTypeStatistics implements Serializable {

    private String elpModelId;
    private String elpType;
    private String elpDesc;
    private Long count;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public String getElpModelId() {
        return elpModelId;
    }

    public void setElpModelId(String elpModelId) {
        this.elpModelId = elpModelId;
    }

    public String getElpType() {
        return elpType;
    }

    public void setElpType(String elpType) {
        this.elpType = elpType;
    }

    public String getElpDesc() {
        return elpDesc;
    }

    public void setElpDesc(String elpDesc) {
        this.elpDesc = elpDesc;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
