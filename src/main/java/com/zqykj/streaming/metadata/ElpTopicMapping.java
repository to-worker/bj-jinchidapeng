package com.zqykj.streaming.metadata;

import java.io.Serializable;

/**
 * Created by alfer on 9/4/17.
 */
public class ElpTopicMapping implements Serializable {

    /**
     * Kafka topic
     */
    private String topic;

    /**
     * ELPModel
     */
    private String modelId;

    /**
     * 标识是实体数据或者链接数据 -> 0: 即是实体数据也是链接数据, 1: 实体数据, 2: 链接数据
     */
    private Integer type;

    /**
     * 模型中实体的uuid
     */
    private String entityUUID;

    /**
     * 模型中链接的uuid
     */
    private String linkUUID;

    public ElpTopicMapping() {
    }

    public ElpTopicMapping(String topic, String modelId, Integer type, String entityUUID, String linkUUID) {
        this.topic = topic;
        this.modelId = modelId;
        this.type = type;
        this.entityUUID = entityUUID;
        this.linkUUID = linkUUID;
    }

    @Override
    public String toString() {
        return "ElpTopicMapping{" +
                "topic='" + topic + '\'' +
                ", modelId='" + modelId + '\'' +
                ", type=" + type +
                ", entityUUID='" + entityUUID + '\'' +
                ", linkUUID='" + linkUUID + '\'' +
                '}';
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public String getEntityUUID() {
        return entityUUID;
    }

    public void setEntityUUID(String entityUUID) {
        this.entityUUID = entityUUID;
    }

    public String getLinkUUID() {
        return linkUUID;
    }

    public void setLinkUUID(String linkUUID) {
        this.linkUUID = linkUUID;
    }
}
