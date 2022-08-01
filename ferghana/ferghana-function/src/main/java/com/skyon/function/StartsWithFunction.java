package com.skyon.function;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class StartsWithFunction extends ScalarFunction {
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    public Integer eval(String str, String containStr) {
        int flag = 0;
        if (str.startsWith(containStr)) {
            flag = 1;
        }
        return flag;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
