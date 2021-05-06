package com.skyon.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeIntervalFunction extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    public Boolean eval(Timestamp param1, String param2) {
        String beginStr = param2.substring(1, 6);
        String endStr = param2.substring(7, 12);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        try {
            String format = sdf.format(new Date(param1.getTime()));
            Calendar date = Calendar.getInstance();
            date.setTime(new Date(param1.getTime()));
            Calendar begin = Calendar.getInstance();
            begin.setTime(sdf2.parse(format + " " + beginStr));
            Calendar end = Calendar.getInstance();
            end.setTime(sdf2.parse(format + " " + endStr));

            if (end.getTimeInMillis() < begin.getTimeInMillis()) {
                end.add(Calendar.DATE, 1);
            }
            if (date.getTimeInMillis() < begin.getTimeInMillis() && date.getTimeInMillis() < end.getTimeInMillis()) {
                date.add(Calendar.DATE, 1);
            }
            if (param2.startsWith("(") && param2.endsWith(")")) {
                return date.getTimeInMillis() > begin.getTimeInMillis() && date.getTimeInMillis() < end.getTimeInMillis();
            }
            if (param2.startsWith("(") && param2.endsWith("]")) {
                return date.getTimeInMillis() > begin.getTimeInMillis() && date.getTimeInMillis() <= end.getTimeInMillis();
            }
            if (param2.startsWith("[") && param2.endsWith(")")) {
                return date.getTimeInMillis() >= begin.getTimeInMillis() && date.getTimeInMillis() < end.getTimeInMillis();
            }
            if (param2.startsWith("[") && param2.endsWith("]")) {
                return date.getTimeInMillis() >= begin.getTimeInMillis() && date.getTimeInMillis() <= end.getTimeInMillis();
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
