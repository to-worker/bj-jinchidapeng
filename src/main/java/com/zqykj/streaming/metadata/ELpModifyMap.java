package com.zqykj.streaming.metadata;

import com.alibaba.fastjson.JSON;
import com.mongodb.DBObject;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * @author alfer
 * @date 1/26/18
 */
public class ELpModifyMap implements Serializable {

    private static final long serialVersionUID = 6638159280041687359L;

    private String elp;
    private Long seqId;
    private Date createTime;
    private Date updateTime;
    private List<ELpTypeModifyMap> entities;
    private List<ELpTypeModifyMap> links;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public Long getSeqId() {
        return seqId;
    }

    public void setSeqId(Long seqId) {
        this.seqId = seqId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getElp() {
        return elp;
    }

    public void setElp(String elp) {
        this.elp = elp;
    }

    public List<ELpTypeModifyMap> getEntities() {
        return entities;
    }

    public void setEntities(List<ELpTypeModifyMap> entities) {
        this.entities = entities;
    }

    public List<ELpTypeModifyMap> getLinks() {
        return links;
    }

    public void setLinks(List<ELpTypeModifyMap> links) {
        this.links = links;
    }
}





