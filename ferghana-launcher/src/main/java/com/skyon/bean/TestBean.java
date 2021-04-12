package com.skyon.bean;

import java.sql.Timestamp;
import java.sql.Types;

public class TestBean {
    public String produce_id;
    public int number;
    public Timestamp order_time;
    public Timestamp proctime;

    public String getProduce_id() {
        return produce_id;
    }

    public void setProduce_id(String produce_id) {
        this.produce_id = produce_id;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public Timestamp getOrder_time() {
        return order_time;
    }

    public void setOrder_time(Timestamp order_time) {
        this.order_time = order_time;
    }

    public Timestamp getProctime() {
        return proctime;
    }

    public void setProctime(Timestamp proctime) {
        this.proctime = proctime;
    }
}
