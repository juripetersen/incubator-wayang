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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataStreamChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;

/**
 * Implementation of the {@link LocalCallbackSink} operator for the Flink
 * platform.
 */

public class FlinkDataStreamLocalCallbackSink<T extends Serializable> extends LocalCallbackSink<T>
        implements FlinkExecutionOperator {

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkDataStreamLocalCallbackSink(final LocalCallbackSink<T> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final FlinkExecutor flinkExecutor,
            final OptimizationContext.OperatorContext operatorContext) throws Exception {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataStreamChannel.Instance input = (DataStreamChannel.Instance) inputs[0];

        final DataStream<T> dataStreamInput = input.provideDataStream();

        if (this.collector != null) {
            dataStreamInput.executeAndCollect().forEachRemaining(this.collector::add);
        } else {
            dataStreamInput.print();
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.localcallbacksink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataStreamChannel.DESCRIPTOR, DataStreamChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        throw new UnsupportedOperationException(String.format("%s does not have output channels.", this));
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkLocalCallbackSink<T>(this);
    }
}
