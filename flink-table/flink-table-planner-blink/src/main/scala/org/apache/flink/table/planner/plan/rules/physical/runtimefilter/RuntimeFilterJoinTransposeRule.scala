package org.apache.flink.table.planner.plan.rules.physical.runtimefilter

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecCalc, BatchExecHashJoin}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexBuilder, RexCall, RexNode, RexProgram, RexProgramBuilder}
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.functions.sql.internal.SqlRuntimeFilterFunction
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.planner.plan.rules.physical.runtimefilter.BaseRuntimeFilterPushDownRule.findRuntimeFilters
import org.apache.flink.table.planner.plan.rules.physical.runtimefilter.RuntimeFilterJoinTransposeRule._
import org.apache.flink.table.planner.plan.schema.OptimizerFlags

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * @author Eric Fu
 */
class RuntimeFilterJoinTransposeRule extends RelOptRule(
  operand(classOf[BatchExecCalc],
    operand(classOf[BatchExecHashJoin], any())),
  "RuntimeFilterJoinTransposeRule"
) {

  override def matches(call: RelOptRuleCall): Boolean = {
    if (OptimizerFlags.getFlag(OptimizerFlags.DISABLE_RUNTIME_FILTER_JOIN_TRANSPOSE_RULE)) {
      return false
    }

    val calc: BatchExecCalc = call.rel(0)
    val join: BatchExecHashJoin = call.rel(1)
    join.flinkJoinType == FlinkJoinType.INNER &&
      findRuntimeFilters(calc.getProgram).nonEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val calc: BatchExecCalc = call.rel(0)
    val join: BatchExecHashJoin = call.rel(1)
    val offset = join.getLeft.getRowType.getFieldCount
    val conditions = RelOptUtil.conjunctions(
      calc.getProgram.expandLocalRef(calc.getProgram.getCondition))
    val builder = call.builder()
    val rexBuilder = calc.getCluster.getRexBuilder
    val origFields = join.getRowType.getFieldList

    val leftPushedConditions = new mutable.ArrayBuffer[RexNode]()
    val rightPushedConditions = new mutable.ArrayBuffer[RexNode]()
    val remainingConditions = new mutable.ArrayBuffer[RexNode]()

    for (condition <- conditions) {
      val rCols = RelOptUtil.InputFinder.bits(condition)
      if (rCols.forall(_ < offset)) {
        // push to left
        leftPushedConditions.add(condition)
      } else if (rCols.forall(_ >= offset)) {
        // push to right
        val adjustments = Array.fill(origFields.size())(-offset)
        rightPushedConditions.add(condition.accept(new RelOptUtil.RexInputConverter(rexBuilder,
          origFields,
          join.getRight.getRowType.getFieldList,
          adjustments
        )))
      } else {
        remainingConditions.add(condition)
      }
    }

    if (leftPushedConditions.isEmpty && rightPushedConditions.isEmpty) {
      return
    }

    val leftInput = if (leftPushedConditions.nonEmpty) {
      pushOneSide(join.getLeft, leftPushedConditions, builder, rexBuilder)
    } else {
      join.getLeft
    }
    val rightInput = if (rightPushedConditions.nonEmpty) {
      pushOneSide(join.getRight, rightPushedConditions, builder, rexBuilder)
    } else {
      join.getRight
    }
    val newJoin = join.copy(join.getTraitSet, join.getCondition,
      leftInput, rightInput, join.getJoinType, join.isSemiJoinDone)

    val pBuilder = RexProgramBuilder.forProgram(calc.getProgram, rexBuilder, true)
    pBuilder.clearCondition()
    if (remainingConditions.nonEmpty) {
      pBuilder.addCondition(builder.and(remainingConditions))
    }
    val newCalc = calc.copy(calc.getTraitSet, newJoin, pBuilder.getProgram)
    call.transformTo(newCalc)
  }

  private def pushOneSide(filterInput: RelNode, conditions: mutable.ArrayBuffer[RexNode],
                          builder: RelBuilder, rexBuilder: RexBuilder): RelNode = {
    val rexProgramBuilder = new RexProgramBuilder(filterInput.getRowType, rexBuilder)
    rexProgramBuilder.addCondition(builder.and(conditions))
    InsertRuntimeFilterRule.projectAllFields(filterInput, rexProgramBuilder)
    val newProgram = rexProgramBuilder.getProgram

    updateRuntimeFilterFunction(filterInput, newProgram)

    new BatchExecCalc(
      filterInput.getCluster,
      filterInput.getTraitSet,
      filterInput,
      newProgram,
      filterInput.getRowType)
  }

}

object RuntimeFilterJoinTransposeRule {

  val INSTANCE: RelOptRule = new RuntimeFilterJoinTransposeRule

  def updateRuntimeFilterFunction(filterInput: RelNode, program: RexProgram): Unit = {
    // update ndv
    val rfCalls = findRuntimeFilters(program)
    rfCalls.foreach { call =>
      val fieldIndex = getIndexFromCall(call)
      val rf = call.getOperator.asInstanceOf[SqlRuntimeFilterFunction]
      val query = filterInput.getCluster.getMetadataQuery
      val ndv = query.getDistinctRowCount(filterInput, ImmutableBitSet.of(fieldIndex), null)
      if (ndv != null) {
        rf.ndv = ndv
      }
      val rowCount = query.getRowCount(filterInput)
      if (rowCount != null) {
        rf.rowCount = rowCount
      }
    }
  }

  def getIndexFromCall(call: RexCall): Int = {
    val indexArray = RelOptUtil.InputFinder.bits(call).toArray
    require(indexArray.length == 1)
    indexArray.head
  }

}
