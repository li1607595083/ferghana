package com.skyon.project.system.domain.vo;


import com.alibaba.excel.annotation.ExcelProperty;
import com.alibaba.excel.metadata.BaseRowModel;
import lombok.Data;

import java.io.Serializable;

// 信用卡重要指标数据vo
@Data
public class Model1  extends BaseRowModel implements Serializable {
    private static final long serialVersionUID = 1L;
    @ExcelProperty(value = { "机构名称","机构名称","机构名称"},index = 0)
    private  String name;

    @ExcelProperty(value = { "垫款金额","垫款金额","今日"},index = 1)
    private  String name1;

    @ExcelProperty(value = { "垫款金额","垫款金额","较上日"},index= 2)
    private  String name2;

    @ExcelProperty(value = { "垫款金额","垫款金额","较上月"}, index=3)
    private  String name3;
    @ExcelProperty(value = { "垫款金额","垫款金额","较上年"},index=4)
    private  String name4;
    @ExcelProperty(value = { "不计息本金","不计息本金","当日"},index=5)
    private  String name5;
    @ExcelProperty(value = { "不计息本金","不计息本金","较上日"},index=6)
    private  String name6;
    @ExcelProperty(value = { "不计息本金","不计息本金","较上月"},index=7)
    private  String name7;
    @ExcelProperty(value = { "不计息本金","不计息本金","较上年"},index=8)
    private  String name8;
    @ExcelProperty(value = { "消费信贷余额","消费信贷余额","当日"},index=9)
    private  String name9;
    @ExcelProperty(value = { "消费信贷余额","消费信贷余额","较上日"},index=10)
    private  String name10;
    @ExcelProperty(value = { "消费信贷余额","消费信贷余额","较上月"},index=11)
    private  String name11;
    @ExcelProperty(value = { "消费信贷余额","消费信贷余额","较上年"},index=12)
    private  String name12;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName5() {
        return name5;
    }

    public void setName5(String name5) {
        this.name5 = name5;
    }

    public String getName6() {
        return name6;
    }

    public void setName6(String name6) {
        this.name6 = name6;
    }

    public String getName7() {
        return name7;
    }

    public void setName7(String name7) {
        this.name7 = name7;
    }

    public String getName8() {
        return name8;
    }

    public void setName8(String name8) {
        this.name8 = name8;
    }

    public String getName9() {
        return name9;
    }

    public void setName9(String name9) {
        this.name9 = name9;
    }

    public String getName10() {
        return name10;
    }

    public void setName10(String name10) {
        this.name10 = name10;
    }

    public String getName11() {
        return name11;
    }

    public void setName11(String name11) {
        this.name11 = name11;
    }

    public String getName12() {
        return name12;
    }

    public void setName12(String name12) {
        this.name12 = name12;
    }

    public String getName1() {
        return name1;
    }

    public void setName1(String name1) {
        this.name1 = name1;
    }

    public String getName2() {
        return name2;
    }

    public void setName2(String name2) {
        this.name2 = name2;
    }

    public String getName3() {
        return name3;
    }

    public void setName3(String name3) {
        this.name3 = name3;
    }

    public String getName4() {
        return name4;
    }

    public void setName4(String name4) {
        this.name4 = name4;
    }
}
