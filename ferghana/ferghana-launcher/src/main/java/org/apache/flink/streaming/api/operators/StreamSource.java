/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org_change.org.apache.flink.streaming.api.operators;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Output;
import org_change.org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.concurrent.ScheduledFuture;

/**
 * @desc 对 run(...) 方法，其中对添加行进行了的说明
 */
@Internal
public class StreamSource<OUT, SRC extends SourceFunction<OUT>> extends AbstractUdfStreamOperator<OUT, SRC> {

    private static final long serialVersionUID = 1L;

    private transient SourceFunction.SourceContext<OUT> ctx;

    private transient volatile boolean canceledOrStopped = false;

    private transient volatile boolean hasSentMaxWatermark = false;

    public StreamSource(SRC sourceFunction) {
        super(sourceFunction);

        this.chainingStrategy = ChainingStrategy.HEAD;
    }



    /**
     * @desc 相应的改变，请看代码注释 //....
     */
    public void run(final Object lockingObject,
                    final StreamStatusMaintainer streamStatusMaintainer,
                    final OperatorChain<?, ?> operatorChain) throws Exception {

        run(lockingObject, streamStatusMaintainer, output, operatorChain);
    }


    public void run(final Object lockingObject,
                    final StreamStatusMaintainer streamStatusMaintainer,
                    final Output<StreamRecord<OUT>> collector,
                    final OperatorChain<?, ?> operatorChain) throws Exception {

        final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();

        final Configuration configuration = this.getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
        final long latencyTrackingInterval = getExecutionConfig().isLatencyTrackingConfigured()
                ? getExecutionConfig().getLatencyTrackingInterval()
                : configuration.getLong(MetricOptions.LATENCY_INTERVAL);

        LatencyMarksEmitter<OUT> latencyEmitter = null;
        if (latencyTrackingInterval > 0) {
            latencyEmitter = new LatencyMarksEmitter<>(
                    getProcessingTimeService(),
                    collector,
                    latencyTrackingInterval,
                    this.getOperatorID(),
                    getRuntimeContext().getIndexOfThisSubtask());
        }

        final long watermarkInterval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

        // 获取参数类(ParameterTool)
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        // idleTimeout: 表示如果数据源没有新的记录，那么将触发发出新的 WaterMark，其中 -1 表示不开启此机制
        long idleTimeout = Long.parseLong(parameterTool.get("idleTimeout", -1 + ""));
        // 此时间为双流join时，注册延迟触发时间
        long twoStreamJoinDelayTime = Long.parseLong(parameterTool.get("two_stream_join_delay_time", 0 + ""));
        // delayTime: WaterMark的延迟触发时间，当数据源的没有产生新的记录时,
        // 新发出的 WaterMark 必定会触发所有的计算
        long delayTime = getDelayTime();

        if(delayTime == 0 && twoStreamJoinDelayTime == 0){
            this.ctx = StreamSourceContexts.getSourceContext(
                    timeCharacteristic,
                    getProcessingTimeService(),
                    lockingObject,
                    streamStatusMaintainer,
                    collector,
                    watermarkInterval,
                    idleTimeout);
        } else {
            //调用了 StreamSourceContexts 重载方法 getSourceContext(...)，主要是为了传入 delayTime 此参数值
            this.ctx = StreamSourceContexts.getSourceContext(
                    timeCharacteristic,
                    getProcessingTimeService(),
                    lockingObject,
                    streamStatusMaintainer,
                    collector,
                    watermarkInterval,
                    idleTimeout,
                    delayTime,
                    twoStreamJoinDelayTime
                    );
        }


        try {
            userFunction.run(ctx);

            // if we get here, then the user function either exited after being done (finite source)
            // or the function was canceled or stopped. For the finite source case, we should emit
            // a final watermark that indicates that we reached the end of event-time, and end inputs
            // of the operator chain
            if (!isCanceledOrStopped()) {
                // in theory, the subclasses of StreamSource may implement the BoundedOneInput interface,
                // so we still need the following call to end the input
                synchronized (lockingObject) {
                    operatorChain.endHeadOperatorInput(1);
                }
            }
        } finally {
            if (latencyEmitter != null) {
                latencyEmitter.close();
            }
        }
    }

    /**
     * @desc 获取 WaterMark 延迟触发时间，从人物名中，获取 waterMark 参数值
     * @return
     */
    private long getDelayTime() {
        String delayTime = 0 + "";
        for (String s : this.getContainingTask().getName().split("->")) {
            s = s.trim();
            if (s.startsWith("WatermarkAssigner")){
                String s1 = s.split(",", 2)[1]
                        .split(":INTERVAL SECOND")[0];
                String outTime = StringUtils.reverse(s1).split("\\s+", 2)[0];
                delayTime = StringUtils.reverse(outTime);
            }
        }
        return Long.parseLong(delayTime);
    }

    public void advanceToEndOfEventTime() {
        if (!hasSentMaxWatermark) {
            ctx.emitWatermark(Watermark.MAX_WATERMARK);
            hasSentMaxWatermark = true;
        }
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
            if (!isCanceledOrStopped() && ctx != null) {
                advanceToEndOfEventTime();
            }
        } finally {
            // make sure that the context is closed in any case
            if (ctx != null) {
                ctx.close();
            }
        }
    }

    public void cancel() {
        // important: marking the source as stopped has to happen before the function is stopped.
        // the flag that tracks this status is volatile, so the memory model also guarantees
        // the happens-before relationship
        markCanceledOrStopped();
        userFunction.cancel();

        // the context may not be initialized if the source was never running.
        if (ctx != null) {
            ctx.close();
        }
    }

    /**
     * Marks this source as canceled or stopped.
     *
     * <p>This indicates that any exit of the {@link #(Object, StreamStatusMaintainer, Output)} method
     * cannot be interpreted as the result of a finite source.
     */
    protected void markCanceledOrStopped() {
        this.canceledOrStopped = true;
    }

    /**
     * Checks whether the source has been canceled or stopped.
     * @return True, if the source is canceled or stopped, false is not.
     */
    protected boolean isCanceledOrStopped() {
        return canceledOrStopped;
    }

    private static class LatencyMarksEmitter<OUT> {
        private final ScheduledFuture<?> latencyMarkTimer;

        public LatencyMarksEmitter(
                final ProcessingTimeService processingTimeService,
                final Output<StreamRecord<OUT>> output,
                long latencyTrackingInterval,
                final OperatorID operatorId,
                final int subtaskIndex) {

            latencyMarkTimer = processingTimeService.scheduleAtFixedRate(
                    new ProcessingTimeCallback() {
                        @Override
                        public void onProcessingTime(long timestamp) throws Exception {
                            try {
                                // ProcessingTimeService callbacks are executed under the checkpointing lock
                                output.emitLatencyMarker(new LatencyMarker(processingTimeService.getCurrentProcessingTime(), operatorId, subtaskIndex));
                            } catch (Throwable t) {
                                // we catch the Throwables here so that we don't trigger the processing
                                // timer services async exception handler
                                AbstractStreamOperator.LOG.warn("Error while emitting latency marker.", t);
                            }
                        }
                    },
                    0L,
                    latencyTrackingInterval);
        }

        public void close() {
            latencyMarkTimer.cancel(true);
        }
    }
}
