//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.flink.api.common.serialization;

import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

@Public
public interface DeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {
    @PublicEvolving
    default void open(DeserializationSchema.InitializationContext context) throws Exception {
    }

    T deserialize(byte[] var1) throws IOException;

    /**
     * @param var1
     * @param fieldname
     * @param fieldvalue
     * @return
     * @throws IOException
     * @description 新增的接口默认方法
     */
    default T deserialize(byte[] var1, String fieldname, String fieldvalue) throws IOException {
        return null;
    }


    @PublicEvolving
    default void deserialize(byte[] message, Collector<T> out) throws IOException {
        T deserialize = this.deserialize(message);
        if (deserialize != null) {
            out.collect(deserialize);
        }

    }

    /**
     * @param message
     * @param out
     * @param fieldname
     * @param fieldvalue
     * @throws IOException
     * @description 新增接口默认方法，主要对上游序列化数据添加字段，用以下游注册成新表使用
     * eg: 添加自定义主键
     */
    @PublicEvolving
    default void deserialize(byte[] message, Collector<T> out, String fieldname, String fieldvalue) throws IOException {
        T deserialize = this.deserialize(message, fieldname, fieldvalue);
        if (deserialize != null) {
            out.collect(deserialize);
        }

    }

    boolean isEndOfStream(T var1);

    @PublicEvolving
    public interface InitializationContext {
        MetricGroup getMetricGroup();
    }
}
