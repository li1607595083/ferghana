/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.over;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.parser.LongParser;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * @Change 新增了成员变量 twoStreamJoinDelaySendTime，以及修改了此类的构造方法，open(...), processElement(...)
 * , onTimer(...) 以及registerCleanupTimer(...) 方法 进行相应的修改；
 */
public class RowTimeRangeBoundedPrecedingFunction<K> extends KeyedProcessFunction<K, RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RowTimeRangeBoundedPrecedingFunction.class);

    private final GeneratedAggsHandleFunction genAggsHandler;
    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final long precedingOffset;
    private final int rowTimeIdx;
    private FastDateFormat instance;
    private final String type;
    private long twoStreamJoinDelaySendTime = 0;

    private transient JoinedRowData output;

    // the state which keeps the last triggering timestamp
    private transient ValueState<Long> lastTriggeringTsState;

    // the state which used to materialize the accumulator for incremental calculation
    private transient ValueState<RowData> accState;

    // the state which keeps the safe timestamp to cleanup states
    private transient ValueState<Long> cleanupTsState;

    // the state which keeps all the data that are not expired.
    // The first element (as the mapState key) of the tuple is the time stamp. Per each time stamp,
    // the second element of tuple is a list that contains the entire data of all the rows belonging
    // to this time stamp.
    private transient MapState<Long, List<RowData>> inputState;

    private transient AggsHandleFunction function;

    /**
     * @Change 增加了窗口统计方式
     */
    public RowTimeRangeBoundedPrecedingFunction(
            GeneratedAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            long precedingOffset,
            int rowTimeIdx) {
        Preconditions.checkNotNull(precedingOffset);
        this.genAggsHandler = genAggsHandler;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.rowTimeIdx = rowTimeIdx;
        switch (precedingOffset + ""){
            // 以日为统计的窗口时间
            case "46620000": this.type = "01";this.precedingOffset = 1*24*60*60*1000L;break;
            // 以周为统计的窗口时间
            case "53280000": this.type = "02";this.precedingOffset = 7*24*60*60*1000L;break;
            // 以月为统计的窗口时间
            case "59940000": this.type = "03";this.precedingOffset = 31*24*60*60*1000L;break;
            // 常规的统计时间
            default: this.type = "04";
                this.precedingOffset = precedingOffset;
                break;
        }
    }

    /**
     * @Change 新增加了两部分, 1.如果是双流join的,那么计算触发时间会被延迟
     *                          2.通过窗口计算类型，来创建相应的 FastDateFormat 实例
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //  如果是双流join，需要对注册时间延后，且延后触发时间放在taskName里面
        ParameterTool globalJobParameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        this.twoStreamJoinDelaySendTime = Long.parseLong(globalJobParameters.getProperties().getProperty("TWO_STREAM_JOIN_DELAY_TIME", "0"));
        switch (type){
            // 以日为统计的窗口时间
            case "01": instance = FastDateFormat.getInstance("yyyy-MM-dd");break;
            // 以周为统计的窗口时间
            case "02": instance = FastDateFormat.getInstance("yyyy-MM-dd");break;
            // 以月为统计的窗口时间
            case "03": instance = FastDateFormat.getInstance("yyyy-MM");break;
        }
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        output = new JoinedRowData();

        ValueStateDescriptor<Long> lastTriggeringTsDescriptor = new ValueStateDescriptor<Long>(
                "lastTriggeringTsState",
                Types.LONG);
        lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);

        RowDataTypeInfo accTypeInfo = new RowDataTypeInfo(accTypes);
        ValueStateDescriptor<RowData> accStateDesc = new ValueStateDescriptor<RowData>("accState", accTypeInfo);
        accState = getRuntimeContext().getState(accStateDesc);

        // input element are all binary row as they are came from network
        RowDataTypeInfo inputType = new RowDataTypeInfo(inputFieldTypes);
        ListTypeInfo<RowData> rowListTypeInfo = new ListTypeInfo<RowData>(inputType);
        MapStateDescriptor<Long, List<RowData>> inputStateDesc = new MapStateDescriptor<Long, List<RowData>>(
                "inputState",
                Types.LONG,
                rowListTypeInfo);
        inputState = getRuntimeContext().getMapState(inputStateDesc);

        ValueStateDescriptor<Long> cleanupTsStateDescriptor = new ValueStateDescriptor<>(
                "cleanupTsState",
                Types.LONG
        );
        this.cleanupTsState = getRuntimeContext().getState(cleanupTsStateDescriptor);
    }

    /**
     * @Change 对触发计算时间进行延迟, 如果是非双流 JOIN 操作, 那么延迟触发时间就为0
     */
    @Override
    public void processElement(
            RowData input,
            KeyedProcessFunction<K, RowData, RowData>.Context ctx,
            Collector<RowData> out) throws Exception {
        // triggering timestamp for trigger calculation
        long triggeringTs = input.getLong(rowTimeIdx);
        Long lastTriggeringTs = lastTriggeringTsState.value();
        if (lastTriggeringTs == null) {
            lastTriggeringTs = 0L;
        }

        // check if the data is expired, if not, save the data and register event time timer
        if (triggeringTs > lastTriggeringTs) {
            List<RowData> data = inputState.get(triggeringTs);
            if (null != data) {
                data.add(input);
                inputState.put(triggeringTs, data);
            } else {
                data = new ArrayList<RowData>();
                data.add(input);
                inputState.put(triggeringTs, data);
                // register event time timer
                // 增加延迟触发时间
                ctx.timerService().registerEventTimeTimer(triggeringTs + twoStreamJoinDelaySendTime);
            }
            registerCleanupTimer(ctx, triggeringTs);
        }
    }

    /**
     * @Change 对清除过期转态时间进行延迟, 如果是非双流 JOIN 操作, 那么延迟触发时间就为0
     */
    private void registerCleanupTimer(
            KeyedProcessFunction<K, RowData, RowData>.Context ctx,
            long timestamp) throws Exception {
        // calculate safe timestamp to cleanup states
        long minCleanupTimestamp = timestamp + precedingOffset + 1;
        long maxCleanupTimestamp = timestamp + (long) (precedingOffset * 1.5) + 1;
        // update timestamp and register timer if needed
        Long curCleanupTimestamp = cleanupTsState.value();
        if (curCleanupTimestamp == null || curCleanupTimestamp < minCleanupTimestamp) {
            // we don't delete existing timer since it may delete timer for data processing
            // TODO Use timer with namespace to distinguish timers
            // 增加延迟触发时间
            ctx.timerService().registerEventTimeTimer(maxCleanupTimestamp + twoStreamJoinDelaySendTime);
            cleanupTsState.update(maxCleanupTimestamp);
        }
    }

    /**
     * @Change 根据计算类型，确定计算范围
     */
    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out) throws Exception {
        // 恢复原本实际的时间, 避免影响计算
        timestamp = timestamp - twoStreamJoinDelaySendTime;
        Long cleanupTimestamp = cleanupTsState.value();
        // if cleanupTsState has not been updated then it is safe to cleanup states
        if (cleanupTimestamp != null && cleanupTimestamp <= timestamp) {
            inputState.clear();
            accState.clear();
            lastTriggeringTsState.clear();
            cleanupTsState.clear();
            function.cleanup();
            return;
        }
        // gets all window data from state for the calculation
        List<RowData> inputs = inputState.get(timestamp);
        if (null != inputs) {

            int dataListIndex = 0;
            RowData accumulators = accState.value();

            // initialize when first run or failover recovery per key
            if (null == accumulators) {
                accumulators = function.createAccumulators();
            }
            // set accumulators in context first
            function.setAccumulators(accumulators);

            // keep up timestamps of retract data
            List<Long> retractTsList = new ArrayList<Long>();

            // do retraction
            // 根据计算类型类确定相应的计算范围
            Iterator<Long> dataTimestampIt = inputState.keys().iterator();
            switch (type){
                case "01":
                    long startTimeDay = instance.parse(instance.format(timestamp - 8 * 60 * 60 * 1000)).getTime();
                    while (dataTimestampIt.hasNext()) {
                        Long dataTs = dataTimestampIt.next();
                        if (dataTs - 8 * 60 * 60 * 1000 < startTimeDay) {
                            retraceData(retractTsList, dataTs);
                        }
                    }break;
                case "03":
                    long startTimeMonth = instance.parse(instance.format(timestamp - 8 * 60 * 60 * 100)).getTime();
                    while (dataTimestampIt.hasNext()) {
                        Long dataTs = dataTimestampIt.next();
                        if (dataTs - 8 * 60 * 60 * 1000 < startTimeMonth) {
                            retraceData(retractTsList, dataTs);
                        }
                    }break;
                case "02":
                    Calendar cal = Calendar.getInstance();
                    cal.setTimeInMillis(instance.parse(instance.format(timestamp - 8 * 60 * 60 * 1000)).getTime());
                    cal.setFirstDayOfWeek(Calendar.MONDAY);
                    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                    //获取星期一开始时间戳
                    long startTimeEveryWeek = cal.getTime().getTime();
                    while (dataTimestampIt.hasNext()) {
                        Long dataTs = dataTimestampIt.next();
                        if (dataTs - 8 * 60 * 60 * 1000 < startTimeEveryWeek) {
                            retraceData(retractTsList, dataTs);
                        }
                    }
                    break;
                case  "04":
                    while (dataTimestampIt.hasNext()) {
                        Long dataTs = dataTimestampIt.next();
                        Long offset = timestamp - dataTs;
                        if (offset > precedingOffset) {
                            retraceData(retractTsList, dataTs);
                        }
                    }break;
            }


            // do accumulation
            dataListIndex = 0;
            while (dataListIndex < inputs.size()) {
                RowData curRow = inputs.get(dataListIndex);
                // accumulate current row
                function.accumulate(curRow);
                dataListIndex += 1;
            }

            // get aggregate result
            RowData aggValue = function.getValue();

            // copy forwarded fields to output row and emit output row
            dataListIndex = 0;
            while (dataListIndex < inputs.size()) {
                RowData curRow = inputs.get(dataListIndex);
                output.replace(curRow, aggValue);
                out.collect(output);
                dataListIndex += 1;
            }

            // remove the data that has been retracted
            dataListIndex = 0;
            while (dataListIndex < retractTsList.size()) {
                inputState.remove(retractTsList.get(dataListIndex));
                dataListIndex += 1;
            }

            // update the value of accumulators for future incremental computation
            accumulators = function.getAccumulators();
            accState.update(accumulators);
        }
        lastTriggeringTsState.update(timestamp);
    }

    private void retraceData(List<Long> retractTsList, Long dataTs) throws Exception {
        int dataListIndex;
        List<RowData> retractDataList = inputState.get(dataTs);
        if (retractDataList != null) {
            dataListIndex = 0;
            while (dataListIndex < retractDataList.size()) {
                RowData retractRow = retractDataList.get(dataListIndex);
                function.retract(retractRow);
                dataListIndex += 1;
            }
            retractTsList.add(dataTs);
        } else {
            // Does not retract values which are outside of window if the state is cleared already.
            LOG.warn("The state is cleared because of state ttl. " +
                    "This will result in incorrect result. " +
                    "You can increase the state ttl to avoid this.");
        }
    }

    @Override
    public void close() throws Exception {
        if (null != function) {
            function.close();
        }
    }
}
