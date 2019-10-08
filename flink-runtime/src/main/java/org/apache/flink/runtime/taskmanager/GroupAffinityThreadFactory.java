package org.apache.flink.runtime.taskmanager;

import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityStrategy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;

/**
 * cpu-affinity thread factory
 */
public class GroupAffinityThreadFactory implements ThreadFactory {
	/** The class logger. */
	private static final Logger LOG = LoggerFactory.getLogger(GroupAffinityThreadFactory.class);

	private final ThreadGroup group;
	private final boolean daemon;
	@NotNull
	private final AffinityStrategy[] strategies;
	@Nullable
	private AffinityLock lastAffinityLock = null;

	public GroupAffinityThreadFactory(ThreadGroup group, boolean daemon, @NotNull AffinityStrategy... strategies) {
		this.group = group;
		this.daemon = daemon;
		this.strategies = strategies.length == 0 ? new AffinityStrategy[]{AffinityStrategies.ANY} : strategies;
	}

	@NotNull
	@Override
	public synchronized Thread newThread(@NotNull final Runnable r) {
		Thread t = new Thread(group, new Runnable() {
			@Override
			public void run() {
				AffinityLock al = lastAffinityLock == null ? AffinityLock.acquireLock() : lastAffinityLock.acquireLock(strategies);
				try {
					LOG.info("CPU CORE ID: " + al.cpuId());
					if (al.cpuId() >= 0) {
						lastAffinityLock = al;
					}
					r.run();
				} finally {
					al.release();
				}
			}
		});
		t.setDaemon(daemon);
		return t;
	}
}
