package com.zqykj.batch.document.entity;

import java.io.Serializable;
import java.util.List;

/**
 * @author alfer
 * @date 12/15/17
 */
public class PropertyMapping implements Serializable {

    private static final long serialVersionUID = -5026990333758479220L;
    private String id;
    private boolean needRelation;
    private boolean needExtendsion;
    private String sPropertyName;
    private String dPropertyName;
    private List<SourceTargetEntity> sourceTargetEntities;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "PropertyMapping{" + "id='" + id + '\'' + ", needRelation=" + needRelation + ", sPropertyName='"
                + sPropertyName + '\'' + ", dPropertyName='" + dPropertyName + '\'' + ", sourceTargetEntities="
                + sourceTargetEntities + '}';
    }

    public List<SourceTargetEntity> getSourceTargetEntities() {
        return sourceTargetEntities;
    }

    public void setSourceTargetEntities(List<SourceTargetEntity> sourceTargetEntities) {
        this.sourceTargetEntities = sourceTargetEntities;
    }

    public boolean isNeedExtendsion() {
        return needExtendsion;
    }

    public void setNeedExtendsion(boolean needExtendsion) {
        this.needExtendsion = needExtendsion;
    }

    public boolean isNeedRelation() {
        return needRelation;
    }

    public void setNeedRelation(boolean needRelation) {
        this.needRelation = needRelation;
    }

    public String getsPropertyName() {
        return sPropertyName;
    }

    public void setsPropertyName(String sPropertyName) {
        this.sPropertyName = sPropertyName;
    }

    public String getdPropertyName() {
        return dPropertyName;
    }

    public void setdPropertyName(String dPropertyName) {
        this.dPropertyName = dPropertyName;
    }
}
