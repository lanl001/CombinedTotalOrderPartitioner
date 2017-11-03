package com.microfun.llx.tez.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HivePartitioner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class TezTotalOrderPartitioner<K extends WritableComparable<?>, V>
		extends com.microfun.llx.tez.partition.TezTotalOrderPartitioner<K, V> implements Partitioner<K, V>,HivePartitioner<K, V>{
	public TezTotalOrderPartitioner() {
	}

	public void configure(JobConf job) {
		super.setConf(job);
	}

	/**
	 * Set the path to the SequenceFile storing the sorted partition keyset. It must
	 * be the case that for <tt>R</tt> reduces, there are <tt>R-1</tt> keys in the
	 * SequenceFile.
	 * 
	 * @deprecated Use {@link #setPartitionFile(Configuration, Path)} instead
	 */
	@Deprecated
	public static void setPartitionFile(JobConf job, Path p) {
		com.microfun.llx.tez.partition.TezTotalOrderPartitioner.setPartitionFile(job, p);
	}

	/**
	 * Get the path to the SequenceFile storing the sorted partition keyset.
	 * 
	 * @see #setPartitionFile(JobConf,Path)
	 * @deprecated Use {@link #getPartitionFile(Configuration)} instead
	 */
	@Deprecated
	public static String getPartitionFile(JobConf job) {
		return com.microfun.llx.tez.partition.TezTotalOrderPartitioner.getPartitionFile(job);
	}

	@Override
	public int getBucket(K key, V value, int numBuckets) {
		return this.getPartition(key, value, numBuckets);
	}

}
