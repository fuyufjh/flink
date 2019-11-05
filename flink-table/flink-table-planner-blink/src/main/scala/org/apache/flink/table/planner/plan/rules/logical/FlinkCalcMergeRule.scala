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

package org.apache.flink.table.planner.plan.rules.logical

import java.util

import org.apache.flink.table.planner.plan.utils.FlinkRexUtil
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Calc, RelFactories}
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.metadata.{RelMdUtil, RelMetadataQuery}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexNode, RexOver, RexProgramBuilder, RexUtil}
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.functions.sql.internal.{SqlRuntimeFilterBuilderFunction, SqlRuntimeFilterFunction}
import org.apache.flink.table.planner.plan.rules.physical.runtimefilter.RfBuilderJoinTransposeRule.getIndexFromCall

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * This rule is copied from Calcite's [[org.apache.calcite.rel.rules.CalcMergeRule]].
  *
  * Modification:
  * - Condition in the merged program will be simplified if it exists.
  * - Don't merge calcs which contain non-deterministic expr
  */

/**
  * Planner rule that merges a [[Calc]] onto a [[Calc]].
  *
  * <p>The resulting [[Calc]] has the same project list as the upper [[Calc]],
  * but expressed in terms of the lower [[Calc]]'s inputs.
  */
class FlinkCalcMergeRule(relBuilderFactory: RelBuilderFactory) extends RelOptRule(
  operand(classOf[Calc],
    operand(classOf[Calc], any)),
  relBuilderFactory,
  "FlinkCalcMergeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val topCalc: Calc = call.rel(0)
    val bottomCalc: Calc = call.rel(1)

    // Don't merge a calc which contains windowed aggregates onto a
    // calc. That would effectively be pushing a windowed aggregate down
    // through a filter.
    val topProgram = topCalc.getProgram
    if (RexOver.containsOver(topProgram)) {
      return false
    }

    // Don't merge Calcs which contain non-deterministic expr
    topProgram.getExprList.forall(RexUtil.isDeterministic) &&
      bottomCalc.getProgram.getExprList.forall(RexUtil.isDeterministic)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val topCalc: Calc = call.rel(0)
    val bottomCalc: Calc = call.rel(1)

    val topProgram = topCalc.getProgram
    val rexBuilder = topCalc.getCluster.getRexBuilder
    // Merge the programs together.
    val mergedProgram = RexProgramBuilder.mergePrograms(
      topCalc.getProgram, bottomCalc.getProgram, rexBuilder)
    require(mergedProgram.getOutputRowType eq topProgram.getOutputRowType)

    val newMergedProgram = if (mergedProgram.getCondition != null) {
      val condition = mergedProgram.expandLocalRef(mergedProgram.getCondition)
      val simplifiedCondition = FlinkRexUtil.simplify(rexBuilder, condition)
      val reorderedCondition = reorderRuntimeFilters(rexBuilder, simplifiedCondition, topCalc.getCluster.getMetadataQuery, bottomCalc.getInput)
      if (reorderedCondition.toString == condition.toString) {
        mergedProgram
      } else {
        val programBuilder = RexProgramBuilder.forProgram(mergedProgram, rexBuilder, true)
        programBuilder.clearCondition()
        programBuilder.addCondition(reorderedCondition)
        programBuilder.getProgram(true)
      }
    } else {
      mergedProgram
    }

    val newCalc = topCalc.copy(topCalc.getTraitSet, bottomCalc.getInput, newMergedProgram)
    if (newCalc.getDigest == bottomCalc.getDigest) {
      // newCalc is equivalent to bottomCalc,
      // which means that topCalc
      // must be trivial. Take it out of the game.
      call.getPlanner.setImportance(topCalc, 0.0)
    }
    call.transformTo(newCalc)
  }

  private def reorderRuntimeFilters(rexBuilder: RexBuilder, condition: RexNode,
                                    mq: RelMetadataQuery, input: RelNode): RexNode = {
    val before = RelOptUtil.conjunctions(condition)
    val rfCalls = new mutable.ArrayBuffer[RexNode]
    val rfBuilderCalls = new mutable.ArrayBuffer[RexNode]
    val others = new mutable.ArrayBuffer[RexNode]
    before.foreach {
      case call: RexCall if call.getOperator.isInstanceOf[SqlRuntimeFilterFunction] => rfCalls.add(call)
      case call: RexCall if call.getOperator.isInstanceOf[SqlRuntimeFilterBuilderFunction] => rfBuilderCalls.add(call)
      case other => others.add(other)
    }

    if (rfCalls.isEmpty && rfBuilderCalls.isEmpty) {
      return condition // do not need to reorder
    }

    /*
    if (rfBuilderCalls.nonEmpty) {
      // update ndv & rowCount of RuntimeFilterBuilder
      val pred = RexUtil.composeConjunction(rexBuilder, rfCalls ++ others)
      val rowCount = RelMdUtil.estimateFilteredRows(input, pred, mq)
      for (rfb <- rfBuilderCalls) {
        val rfbf = rfb.asInstanceOf[RexCall].getOperator.asInstanceOf[SqlRuntimeFilterBuilderFunction]
        rfbf.rowCount = rowCount
        val fieldIndex = getIndexFromCall(rfb.asInstanceOf[RexCall])
        rfbf.ndv = mq.getDistinctRowCount(input, ImmutableBitSet.of(fieldIndex), pred)
      }
    }
    */

    RexUtil.composeConjunction(rexBuilder, others ++ rfCalls ++ rfBuilderCalls)
  }

}

object FlinkCalcMergeRule {
  val INSTANCE = new FlinkCalcMergeRule(RelFactories.LOGICAL_BUILDER)
}
