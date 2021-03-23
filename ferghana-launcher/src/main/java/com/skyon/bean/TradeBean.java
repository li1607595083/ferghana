package com.skyon.bean;

public class TradeBean {

    public String cust_no;
    public String trade_account;
    public String trade_time;

    public TradeBean() {
    }

    public TradeBean(String cust_no, String trade_account, String trade_time) {
        this.cust_no = cust_no;
        this.trade_account = trade_account;
        this.trade_time = trade_time;
    }

    public static TradeBean of(String cust_no, String trade_account, String trade_time) {
        return new TradeBean(cust_no, trade_account, trade_time);
    }

    @Override
    public String toString() {
        return "TradeBean{" +
                "cust_no='" + cust_no + '\'' +
                ", trade_account='" + trade_account + '\'' +
                ", trade_time='" + trade_time + '\'' +
                '}';
    }
}
