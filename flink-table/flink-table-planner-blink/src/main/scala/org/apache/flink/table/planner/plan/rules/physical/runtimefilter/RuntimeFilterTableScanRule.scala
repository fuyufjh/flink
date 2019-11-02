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

package org.apache.flink.table.planner.plan.rules.physical.runtimefilter

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex._
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.functions.sql.internal.SqlRuntimeFilterFunction
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecCalc, BatchExecTableSourceScan}
import org.apache.flink.table.planner.plan.rules.physical.runtimefilter.BaseRuntimeFilterPushDownRule.findRuntimeFilters
import org.apache.flink.table.planner.plan.schema.{FlinkRelOptTable, TableSourceTable}
import org.apache.flink.table.planner.sources.ParquetTableSource

class RuntimeFilterTableScanRule extends RelOptRule(
  operand(classOf[BatchExecCalc],
    operand(classOf[BatchExecTableSourceScan], none)),
  "RuntimeFilterTableScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val calc: BatchExecCalc = call.rel(0)
    val scan: BatchExecTableSourceScan = call.rel(1)
    findRuntimeFilters(calc.getProgram).nonEmpty &&
      scan.tableSource.isInstanceOf[ParquetTableSource] &&
      !scan.tableSource.asInstanceOf[ParquetTableSource].isFilterPushedDown
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: BatchExecCalc = call.rel(0)
    val scan: BatchExecTableSourceScan = call.rel(1)
    val rfs = findRuntimeFilters(calc.getProgram)

    if (!scan.tableSource.isInstanceOf[ParquetTableSource]) {
      return
    }

    val relOptTable = scan.getTable.unwrap(classOf[FlinkRelOptTable])
    val tableSourceTable = relOptTable.unwrap(classOf[TableSourceTable[BaseRow]])
    val parquetTableSource = tableSourceTable.tableSource.asInstanceOf[ParquetTableSource]
    val selectFieldNames = parquetTableSource.selectFieldNames();

    var newTableSource = parquetTableSource
    for (rf <- rfs) {
      val broadcastId = rf.getOperator.asInstanceOf[SqlRuntimeFilterFunction].getBroadcastId
      val inputRef = rf.getOperands.get(0).asInstanceOf[RexInputRef]
      if (selectFieldNames(inputRef.getIndex).endsWith("date_sk")) {
        newTableSource = newTableSource.applyRuntimeFilter(broadcastId, inputRef.getIndex)
      }
    }

    if (newTableSource == parquetTableSource) {
      return
    }

    val newTableSourceTable = tableSourceTable.replaceTableSource(newTableSource)
    val newRelOptTable = relOptTable.copy(newTableSourceTable, tableSourceTable.getRowType(call.builder.getTypeFactory))

    val newScan = new BatchExecTableSourceScan(scan.getCluster, scan.getTraitSet, newRelOptTable)
    val newCalc = calc.copy(calc.getTraitSet, newScan, calc.getProgram)
    call.transformTo(newCalc)
  }
}

object RuntimeFilterTableScanRule {
  val INSTANCE: RelOptRule = new RuntimeFilterTableScanRule
}

