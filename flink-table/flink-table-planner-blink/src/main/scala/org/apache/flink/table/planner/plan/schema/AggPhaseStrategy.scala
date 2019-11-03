package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.planner.utils.AggregatePhaseStrategy

object AggPhaseStrategy {
  var strategy = AggregatePhaseStrategy.AUTO;
  def get(): AggregatePhaseStrategy = {
    return strategy
  }

  def set(boolean: AggregatePhaseStrategy): Unit = {
    strategy = boolean
  }
}
