/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.calcite.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;


/**
 * Rule that converts {@link LogicalJoin} to Wayang convention
 * {@link WayangJoin}
 */
public class WayangJoinRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalJoin.class, Convention.NONE,
                    WayangConvention.INSTANCE, "WayangJoinRule")
            .withRuleFactory(WayangJoinRule::new);

    protected WayangJoinRule(final Config config) {
        super(config);
    }

    @Override
    public RelNode convert(final RelNode relNode) {
        final LogicalJoin join = (LogicalJoin) relNode;
        final List<RelNode> newInputs = new ArrayList<>();
        for (final RelNode input : join.getInputs()) {
            final RelNode convertedNode = !(input.getConvention() instanceof WayangConvention)
                    ? convert(input, input.getTraitSet().replace(WayangConvention.INSTANCE))
                    : input;

            newInputs.add(convertedNode);
        }

        return new WayangJoin(
                join.getCluster(),
                join.getTraitSet().replace(WayangConvention.INSTANCE),
                newInputs.get(0),
                newInputs.get(1),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType());
    }
}
