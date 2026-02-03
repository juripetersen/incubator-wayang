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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangAggregate;

/**
 * Rule that converts {@link LogicalAggregate} to Wayang convention
 * {@link WayangAggregate}
 */
public class WayangAggregateRule extends ConverterRule {

    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalAggregate.class,
                    Convention.NONE, WayangConvention.INSTANCE,
                    "WayangAggregateRule")
            .withRuleFactory(WayangAggregateRule::new);

    protected WayangAggregateRule(final Config config) {
        super(config);
    }

    @Override
    public RelNode convert(final RelNode relNode) {
        final LogicalAggregate aggregate = (LogicalAggregate) relNode;
        final RelNode input = convert(aggregate.getInput(),
                aggregate.getInput().getTraitSet().replace(WayangConvention.INSTANCE));

        return new WayangAggregate(
                aggregate.getCluster(),
                aggregate.getTraitSet().replace(WayangConvention.INSTANCE),
                aggregate.getHints(),
                input,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList());
    }
}