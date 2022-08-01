package com.skyon.project.system.domain;

import java.math.BigDecimal;

public class TaskManagerLogList {
    private Object name;

    private BigDecimal size;

    private Object applicationId;

    private Object taskmanagerId;

    public Object getName() {
        return name;
    }

    public void setName(Object name) {
        this.name = name;
    }

    public BigDecimal getSize() {
        return size;
    }

    public void setSize(BigDecimal size) {
        this.size = size;
    }

    public Object getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(Object applicationId) {
        this.applicationId = applicationId;
    }

    public Object getTaskmanagerId() {
        return taskmanagerId;
    }

    public void setTaskmanagerId(Object taskmanagerId) {
        this.taskmanagerId = taskmanagerId;
    }
}
