package org.apache.flink.table.planner.sources;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.runtime.parquet.ParquetInputFormat;
import org.apache.flink.table.runtime.util.BloomFilter;
import org.apache.flink.table.runtime.util.RuntimeFilterUtils;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/**
 * @author Eric Fu
 */
public class RuntimeFilterUDP extends UserDefinedPredicate<Long> implements Serializable {

	private static final long serialVersionUID = 4786468246047370063L;

	private static final Logger logger = LoggerFactory.getLogger(RuntimeFilterUDP.class);

	private final String broadcastId;

	private transient boolean initialized = false;
	private transient boolean valid = false;
	private transient long min;
	private transient long max;

	public RuntimeFilterUDP(String broadcastId) {
		this.broadcastId = broadcastId;
	}

	@Override
	public boolean keep(Long value) {
		// Do not support filtering on record-level
		return true;
	}

	@Override
	public boolean canDrop(Statistics<Long> statistics) {
		initialize();
		return valid && (statistics.getMax() < min || statistics.getMin() > max);
	}

	@Override
	public boolean inverseCanDrop(Statistics<Long> statistics) {
		return !canDrop(statistics);
	}

	private void initialize() {
		if (!initialized) {
			StreamingRuntimeContext threadContext = ParquetInputFormat.threadContext.get();
			CompletableFuture<BloomFilter> bfFuture = RuntimeFilterUtils.asyncGetBroadcastBloomFilter(threadContext, broadcastId);
			BloomFilter bf;
			try {
				bf = bfFuture.get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			if (bf != null) {
				min = bf.getValueRange().getMin();
				max = bf.getValueRange().getMax();
				if (min < max) {
					valid = true;
					logger.info("Get range from RuntimeFilter-" + broadcastId + " : [" + min + ", " + max + "]");
				}
			}
			initialized = true;
		}
	}

	@Override
	public String toString() {
		return "RuntimeFilterUDP{broadcastId=" + broadcastId + "}";
	}
}
