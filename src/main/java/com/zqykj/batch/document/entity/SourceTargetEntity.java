package com.zqykj.batch.document.entity;

import java.io.Serializable;
import java.util.List;

/**
 * @author alfer
 * @date 12/21/17
 */
public class SourceTargetEntity implements Serializable {

    private static final long serialVersionUID = -3120636925721863200L;
    private String id;
    private String linkedRelationType;
    private String targetEntityType;
    private boolean onlyFromRel;
    private String description;
    private String sPropertyName;
    private String dPropertyName;
    private String sPropertyId;
    private List<SourceTargetEntity> secondLevel;

    @Override
    public String toString() {
        return "SourceTargetEntity{" + "id='" + id + '\'' + ", linkedRelationType='" + linkedRelationType + '\''
                + ", targetEntityType='" + targetEntityType + '\'' + ", onlyFromRel=" + onlyFromRel + ", description='"
                + description + '\'' + ", sPropertyName='" + sPropertyName + '\'' + ", dPropertyName='" + dPropertyName
                + '\'' + ", sPropertyId='" + sPropertyId + '\'' + ", secondLevel=" + secondLevel + '}';
    }

    public String getdPropertyName() {
        return dPropertyName;
    }

    public void setdPropertyName(String dPropertyName) {
        this.dPropertyName = dPropertyName;
    }

    public List<SourceTargetEntity> getSecondLevel() {
        return secondLevel;
    }

    public void setSecondLevel(List<SourceTargetEntity> secondLevel) {
        this.secondLevel = secondLevel;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLinkedRelationType() {
        return linkedRelationType;
    }

    public void setLinkedRelationType(String linkedRelationType) {
        this.linkedRelationType = linkedRelationType;
    }

    public String getTargetEntityType() {
        return targetEntityType;
    }

    public void setTargetEntityType(String targetEntityType) {
        this.targetEntityType = targetEntityType;
    }

    public boolean isOnlyFromRel() {
        return onlyFromRel;
    }

    public void setOnlyFromRel(boolean onlyFromRel) {
        this.onlyFromRel = onlyFromRel;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getsPropertyName() {
        return sPropertyName;
    }

    public void setsPropertyName(String sPropertyName) {
        this.sPropertyName = sPropertyName;
    }

    public String getsPropertyId() {
        return sPropertyId;
    }

    public void setsPropertyId(String sPropertyId) {
        this.sPropertyId = sPropertyId;
    }
}
