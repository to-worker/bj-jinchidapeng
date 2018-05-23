package com.zqykj.batch.document.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * @author alfer
 * @date 12/15/17
 */
public class DocCardExtractorRule implements Serializable {

    private static final long serialVersionUID = 8362338367245017575L;
    private String id;
    private DocType docType;
    private Date createTime;
    private Date lastMadifyTime;
    private Date lastSynTime;
    private String elpModel;
    private String elpType;
    private String key;
    private String docCollectionName;

    private List<PropertyMapping> propertyMappings;
    private List<PropertyMapping> indexPropertyMappings;

    @Override
    public String toString() {
        return "DocCardExtractorRule{" + "id='" + id + '\'' + ", docType=" + docType + ", createTime=" + createTime
                + ", lastMadifyTime=" + lastMadifyTime + ", lastSynTime=" + lastSynTime + ", elpModel='" + elpModel
                + '\'' + ", elpType='" + elpType + '\'' + ", key='" + key + '\'' + ", docCollectionName='"
                + docCollectionName + '\'' + ", propertyMappings=" + propertyMappings + ", indexPropertyMappings="
                + indexPropertyMappings + '}';
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getElpModel() {
        return elpModel;
    }

    public void setElpModel(String elpModel) {
        this.elpModel = elpModel;
    }

    public String getElpType() {
        return elpType;
    }

    public void setElpType(String elpType) {
        this.elpType = elpType;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public DocType getDocType() {
        return docType;
    }

    public void setDocType(DocType docType) {
        this.docType = docType;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getLastMadifyTime() {
        return lastMadifyTime;
    }

    public void setLastMadifyTime(Date lastMadifyTime) {
        this.lastMadifyTime = lastMadifyTime;
    }

    public Date getLastSynTime() {
        return lastSynTime;
    }

    public void setLastSynTime(Date lastSynTime) {
        this.lastSynTime = lastSynTime;
    }

    public String getDocCollectionName() {
        return docCollectionName;
    }

    public void setDocCollectionName(String docCollectionName) {
        this.docCollectionName = docCollectionName;
    }

    public List<PropertyMapping> getPropertyMappings() {
        return propertyMappings;
    }

    public void setPropertyMappings(List<PropertyMapping> propertyMappings) {
        this.propertyMappings = propertyMappings;
    }

    public List<PropertyMapping> getIndexPropertyMappings() {
        return indexPropertyMappings;
    }

    public void setIndexPropertyMappings(List<PropertyMapping> indexPropertyMappings) {
        this.indexPropertyMappings = indexPropertyMappings;
    }
}
