package org.apache.flink.table.planner.plan.schema;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Eric Fu
 */
public enum OptimizerFlags {

	DISABLE_RUNTIME_FILTER_JOIN_TRANSPOSE_RULE,
	DISABLE_RUNTIME_FILTER_DATA_SK_HACK,
	;

	private static final ThreadLocal<Set<OptimizerFlags>> threadLocalFlags = ThreadLocal.withInitial(HashSet::new);

	public static void clearAllFlags() {
		threadLocalFlags.get().clear();
	}

	public static void setFlag(OptimizerFlags flag) {
		threadLocalFlags.get().add(flag);
	}

	public static boolean getFlag(OptimizerFlags flag) {
		return threadLocalFlags.get().contains(flag);
	}
}
