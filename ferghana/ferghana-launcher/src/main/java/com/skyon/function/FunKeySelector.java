package com.skyon.function;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @description: FunKeySelector
 * @author: ......
 * @create: 2022/4/516:20
 */
public class FunKeySelector implements KeySelector<Tuple3<Integer,String,Long>, Integer> {
    @Override
    public Integer getKey(Tuple3<Integer, String, Long> integerStringLongTuple3) throws Exception {
        return integerStringLongTuple3.f0;
    }
}
