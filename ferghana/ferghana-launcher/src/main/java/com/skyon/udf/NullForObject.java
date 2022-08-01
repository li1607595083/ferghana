package com.skyon.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class NullForObject extends ScalarFunction {

    public String eval() {
        return null;
    }

}
