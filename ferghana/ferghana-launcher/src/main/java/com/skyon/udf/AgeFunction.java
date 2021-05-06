package com.skyon.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Calendar;

public class AgeFunction extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    public Integer eval(String idCardNo) {
        int age;

        Calendar cal = Calendar.getInstance();
        int yearNow = cal.get(Calendar.YEAR);
        int monthNow = cal.get(Calendar.MONTH) + 1;
        int dayNow = cal.get(Calendar.DATE);

        int year = Integer.parseInt(idCardNo.substring(6, 10));
        int month = Integer.parseInt(idCardNo.substring(10, 12));
        int day = Integer.parseInt(idCardNo.substring(12, 14));

        if ((month < monthNow) || (month == monthNow && day <= dayNow)) {
            age = yearNow - year;
        } else {
            age = yearNow - year - 1;
        }

        return age;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}
