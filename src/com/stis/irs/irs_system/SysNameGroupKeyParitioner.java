package com.stis.irs.irs_system;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SysNameGroupKeyParitioner extends Partitioner<SysNameWithNumericKey, IntWritable> {

	@Override
	public int getPartition(SysNameWithNumericKey key, IntWritable val,int numPartitions) {
		int hash=key.getSysName().hashCode();
		int partition =hash % numPartitions;
		return partition;
	}
}
