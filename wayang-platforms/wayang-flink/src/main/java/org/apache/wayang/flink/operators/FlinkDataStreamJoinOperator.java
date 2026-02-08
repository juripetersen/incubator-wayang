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

package org.apache.wayang.flink.operators;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.channels.DataStreamChannel;
import org.apache.wayang.flink.compiler.FunctionCompiler;
import org.apache.wayang.flink.execution.FlinkExecutor;

public class FlinkDataStreamJoinOperator<I0, I1, K> extends JoinOperator<I0, I1, K> implements FlinkExecutionOperator {
    class Joiner implements JoinFunction<I0, I1, Tuple2<I0, I1>> {
        @Override
        public Tuple2<I0, I1> join(final I0 first, final I1 second) throws Exception {
            return new Tuple2<>(first, second);
        }
    }

    final WatermarkStrategy<I0> leftWatermarkStrategy;
    final WatermarkStrategy<I1> rightWatermarkStrategy;
    final Duration duration;

    public FlinkDataStreamJoinOperator(final JoinOperator<I0, I1, K> that) {
        super(that);
        this.leftWatermarkStrategy = WatermarkStrategy
                .<I0>forMonotonousTimestamps()
                .withTimestampAssigner((e, ts) -> 0L);
        this.rightWatermarkStrategy = WatermarkStrategy
                .<I1>forMonotonousTimestamps()
                .withTimestampAssigner((e, ts) -> 0L);
        this.duration = Duration.ofDays(365);
    }

    public FlinkDataStreamJoinOperator(final ProjectionDescriptor<I0, K> descriptor0,
            final ProjectionDescriptor<I1, K> descriptor1) {
        this(descriptor0, descriptor1,
                WatermarkStrategy
                        .<I0>forMonotonousTimestamps()
                        .withTimestampAssigner((e, ts) -> 0L),
                WatermarkStrategy
                        .<I1>forMonotonousTimestamps()
                        .withTimestampAssigner((e, ts) -> 0L),
                Duration.ofDays(365));
        ;
    }

    public FlinkDataStreamJoinOperator(final ProjectionDescriptor<I0, K> descriptor0,
            final ProjectionDescriptor<I1, K> descriptor1, final WatermarkStrategy<I0> leftWatermarkStrategy,
            final WatermarkStrategy<I1> rightWatermarkStrategy, final Duration duration) {
        super(descriptor0, descriptor1);
        this.leftWatermarkStrategy = leftWatermarkStrategy;
        this.rightWatermarkStrategy = rightWatermarkStrategy;
        this.duration = duration;
    }

    public FlinkDataStreamJoinOperator(final SerializableFunction<I0, K> keyExtractor0,
            final SerializableFunction<I1, K> keyExtractor1, final Class<I0> input0Class, final Class<I1> input1Class,
            final Class<K> keyClass, final WatermarkStrategy<I0> leftWatermarkStrategy,
            final WatermarkStrategy<I1> rightWatermarkStrategy, final Duration duration) {
        super(keyExtractor0, keyExtractor1, input0Class, input1Class, keyClass);

        this.leftWatermarkStrategy = leftWatermarkStrategy;
        this.rightWatermarkStrategy = rightWatermarkStrategy;
        this.duration = duration;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(final ChannelInstance[] inputs,
            final ChannelInstance[] kputs, final FlinkExecutor flinkExecutor, final OperatorContext operatorContext)
            throws Exception {
        assert inputs.length == this.getNumInputs();
        assert kputs.length == this.getNumOutputs();

        final DataStreamChannel.Instance input0 = (DataStreamChannel.Instance) inputs[0];
        final DataStreamChannel.Instance input1 = (DataStreamChannel.Instance) inputs[1];
        final DataStreamChannel.Instance output = (DataStreamChannel.Instance) kputs[0];

        final DataStream<I0> dataStream0 = input0.provideDataStream();
        final DataStream<I1> dataStream1 = input1.provideDataStream();

        final DataStream<Tuple2<I0, I1>> outputStream = dataStream0
                .assignTimestampsAndWatermarks(leftWatermarkStrategy)
                .join(dataStream1.assignTimestampsAndWatermarks(rightWatermarkStrategy))
                .where(FunctionCompiler.compileKeySelector(keyDescriptor0))
                .equalTo(FunctionCompiler.compileKeySelector(keyDescriptor1))
                .window(TumblingEventTimeWindows.of(duration))
                .apply(new Joiner());

        output.accept(outputStream);

        return ExecutionOperator.modelLazyExecution(inputs, kputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        // return Collections.singletonList(DataSetChannel.DESCRIPTOR);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }
}
