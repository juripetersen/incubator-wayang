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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataStreamChannel;
import org.apache.wayang.flink.compiler.FunctionCompiler;
import org.apache.wayang.flink.execution.FlinkExecutor;

public class FlinkDataStreamMapOperator<I, O> extends MapOperator<I, O> implements FlinkExecutionOperator {
    public FlinkDataStreamMapOperator(final SerializableFunction<I, O> function, final Class<I> inputTypeClass, final Class<O> outputTypeClass) {
        super(function, inputTypeClass, outputTypeClass);
    }

    public FlinkDataStreamMapOperator(final MapOperator<I, O> that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataStreamChannel.DESCRIPTOR, DataStreamChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataStreamChannel.DESCRIPTOR, DataStreamChannel.DESCRIPTOR_MANY);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(final ChannelInstance[] inputs,
            final ChannelInstance[] outputs, final FlinkExecutor flinkExecutor, final OperatorContext operatorContext) throws Exception {
        final DataStreamChannel.Instance input = (DataStreamChannel.Instance) inputs[0];
        final DataStreamChannel.Instance output = (DataStreamChannel.Instance) outputs[0];

        final DataStream<I> stream = input.provideDataStream();
        final MapFunction<I, O> mapper = FunctionCompiler.compile(this.functionDescriptor);
        final DataStream<O> outputStream = stream.map(mapper).returns(this.getOutputType().getDataUnitType().getTypeClass());

        output.accept(outputStream);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}