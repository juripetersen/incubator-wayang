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
import org.apache.calcite.rel.logical.LogicalFilter;

import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangFilter;

/**
 * Rule that converts {@link LogicalFilter} to Wayang convention
 * {@link WayangFilter}
 */
public class WayangFilterRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalFilter.class,
                    Convention.NONE, WayangConvention.INSTANCE,
                    "WayangFilterRule")
            .withRuleFactory(WayangFilterRule::new);

    protected WayangFilterRule(final Config config) {
        super(config);
    }

    @Override
    public RelNode convert(final RelNode rel) {
        final LogicalFilter filter = (LogicalFilter) rel;
        return new WayangFilter(
                rel.getCluster(),
                rel.getTraitSet().replace(WayangConvention.INSTANCE),
                convert(filter.getInput(), filter.getInput().getTraitSet().replace(WayangConvention.INSTANCE)),
                filter.getCondition());
    }

}
