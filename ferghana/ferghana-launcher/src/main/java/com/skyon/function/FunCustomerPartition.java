package com.skyon.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * @description: FunCustomerPartition
 * @author: ......
 * @create: 2022/4/514:26
 */
public class FunCustomerPartition<T> extends StreamPartitioner<T> {

    private static final long serialVersionUID = 1L;
    private int nextChannelToSendTo = -1;

    @Override
    public StreamPartitioner<T> copy() {
        return this;
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> recordSerializationDelegate) {
        Integer channel = (Integer) ((Tuple3)recordSerializationDelegate.getInstance().getValue()).f0;
        if (channel < this.numberOfChannels){
            return channel;
        } else {
            if (nextChannelToSendTo++ < this.numberOfChannels){
                return nextChannelToSendTo;
            } else {
                return 0;
            }
        }
    }

}
