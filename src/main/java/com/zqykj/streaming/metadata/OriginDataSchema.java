package com.zqykj.streaming.metadata;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

/**
 * Created by alfer on 9/2/17.
 */
public class OriginDataSchema implements Serializable {

    private String typeId;
    private String dsId;
    private String description;
    private Date createTime;
    private Map<String, String> mapping;

    public OriginDataSchema() {
    }

    public String getDsId() {
        return dsId;
    }

    public void setDsId(String dsId) {
        this.dsId = dsId;
    }

    public String getTypeId() {
        return typeId;
    }

    public void setTypeId(String typeId) {
        this.typeId = typeId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Map<String, String> getMapping() {
        return mapping;
    }

    public void setMapping(Map<String, String> mapping) {
        this.mapping = mapping;
    }
}
