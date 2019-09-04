package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

public class LeftJoinToInnerJoinRule extends RelOptRule {
	public static final LeftJoinToInnerJoinRule INNER_LEFT_INSTANCE = new LeftJoinToInnerJoinRule(
		operand(LogicalJoin.class,
			operand(LogicalJoin.class, any()),
			operand(RelNode.class, any())),
		RelFactories.LOGICAL_BUILDER,
		"LeftJoinToInnerJoinRule");


	public LeftJoinToInnerJoinRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
		super(operand, relBuilderFactory, description);
	}

	@Override
	public boolean matches(final RelOptRuleCall call) {
		final LogicalJoin topJoin = call.rel(0);
		final LogicalJoin bottomJoin = call.rel(1);

		return topJoin.getJoinType() == JoinRelType.INNER && bottomJoin.getJoinType() == JoinRelType.LEFT;
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		final LogicalJoin topJoin = call.rel(0);
		final LogicalJoin bottomJoin = call.rel(1);

		final RelNode relC = topJoin.getRight();
		final RelNode relA = bottomJoin.getLeft();
		final RelNode relB = bottomJoin.getRight();

		//        topJoin
		//        /     \
		//   bottomJoin  C
		//    /    \
		//   A      B

		final int aCount = relA.getRowType().getFieldCount();
		final int bCount = relB.getRowType().getFieldCount();
		final int cCount = relC.getRowType().getFieldCount();
		final ImmutableBitSet bBitSet =
			ImmutableBitSet.range(aCount, aCount + bCount);

		// becomes
		//
		//        newTopJoin
		//        /        \
		//   newBottomJoin  B
		//    /    \
		//   A      C

		final List<RexNode> intersecting = new ArrayList<>();
		final List<RexNode> nonIntersecting = new ArrayList<>();
		split(topJoin.getCondition(), bBitSet, intersecting, nonIntersecting);
		if (!intersecting.isEmpty()) {
			// wow, top inner join refer bottom left join right input and top join condition null reject bottom left join right input
			// so we can simplify bottom left join to inner join.
			if (bottomJoin.getJoinType() == JoinRelType.LEFT && topJoin.getJoinType() == JoinRelType.INNER &&
				Strong.isNotTrue(topJoin.getCondition(),bBitSet))  {
				LogicalJoin  newBottomJoin = bottomJoin.copy(bottomJoin.getTraitSet(), bottomJoin.getCondition(), bottomJoin.getLeft(), bottomJoin.getRight(), JoinRelType.INNER, bottomJoin.isSemiJoinDone());
				LogicalJoin newTopJoin = topJoin.copy(topJoin.getTraitSet(), topJoin.getCondition(), newBottomJoin, topJoin.getRight(), topJoin.getJoinType(), topJoin.isSemiJoinDone());
				call.transformTo(newTopJoin);
			}
		}
		return;
	}

	public static void split(
		RexNode condition,
		ImmutableBitSet bitSet,
		List<RexNode> intersecting,
		List<RexNode> nonIntersecting) {
		for (RexNode node : RelOptUtil.conjunctions(condition)) {
			ImmutableBitSet inputBitSet = RelOptUtil.InputFinder.bits(node);
			if (bitSet.intersects(inputBitSet)) {
				intersecting.add(node);
			} else {
				nonIntersecting.add(node);
			}
		}
	}
}
