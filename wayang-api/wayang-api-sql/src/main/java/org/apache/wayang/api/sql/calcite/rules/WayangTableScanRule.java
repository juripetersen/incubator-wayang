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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

import org.apache.wayang.api.sql.calcite.convention.WayangConvention;
import org.apache.wayang.api.sql.calcite.rel.WayangTableScan;

public class WayangTableScanRule extends ConverterRule {
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalTableScan.class,
                    Convention.NONE, WayangConvention.INSTANCE,
                    "WayangTableScanRule")
            .withRuleFactory(WayangTableScanRule::new);

    public static final Config ENUMERABLE_CONFIG = Config.INSTANCE
            .withConversion(TableScan.class,
                    EnumerableConvention.INSTANCE, WayangConvention.INSTANCE,
                    "WayangTableScanRule1")
            .withRuleFactory(WayangTableScanRule::new);

    protected WayangTableScanRule(final Config config) {
        super(config);
    }

    @Override
    public RelNode convert(final RelNode relNode) {

        final TableScan scan = (TableScan) relNode;
        final RelOptTable relOptTable = scan.getTable();

        /**
         * This is quick hack to prevent volcano from merging projects on to TableScans
         * TODO: a cleaner way to handle this
         */
        if (relOptTable.getRowType() == scan.getRowType()) {
            return WayangTableScan.create(scan.getCluster(), relOptTable);
        }
        return null;
    }
}