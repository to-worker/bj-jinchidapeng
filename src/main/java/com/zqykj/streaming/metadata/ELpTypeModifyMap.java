package com.zqykj.streaming.metadata;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * @author alfer
 * @date 1/26/18
 */
public class ELpTypeModifyMap implements Serializable {

    private static final long serialVersionUID = 7941322088065265342L;

    private String elpTypeId;
    private String updateId;
    private String dataSchemaId;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public String getDataSchemaId() {
        return dataSchemaId;
    }

    public void setDataSchemaId(String dataSchemaId) {
        this.dataSchemaId = dataSchemaId;
    }

    public String getElpTypeId() {
        return elpTypeId;
    }

    public void setElpTypeId(String elpTypeId) {
        this.elpTypeId = elpTypeId;
    }

    public String getUpdateId() {
        return updateId;
    }

    public void setUpdateId(String updateId) {
        this.updateId = updateId;
    }
}
