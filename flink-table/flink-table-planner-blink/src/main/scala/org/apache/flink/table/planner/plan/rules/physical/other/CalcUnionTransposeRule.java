package org.apache.flink.table.planner.plan.rules.physical.other;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecUnion;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Eric Fu
 */
public class CalcUnionTransposeRule extends RelOptRule {

	public static final CalcUnionTransposeRule INSTANCE = new CalcUnionTransposeRule();

	private CalcUnionTransposeRule() {
		super(operand(BatchExecCalc.class,
			operand(BatchExecUnion.class, any())), "CalcUnionTransposeRule");
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final BatchExecCalc calc = call.rel(0);
		final BatchExecUnion union = call.rel(1);
		List<RelNode> newInputs = new ArrayList<>();
		for (RelNode input : union.getInputs()) {
			Calc newCalc = calc.copy(calc.getTraitSet(), input, calc.getProgram());
			newInputs.add(newCalc);
		}
		BatchExecUnion newUnion = (BatchExecUnion) union.copy(union.getTraitSet(), newInputs, union.all);
		call.transformTo(newUnion);
	}
}
