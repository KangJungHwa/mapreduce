package com.stis.irs.irs_system;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SysNameWithNumericKeyComparator extends WritableComparator {

	protected SysNameWithNumericKeyComparator() {
		super(SysNameWithNumericKey.class,true);
	}
	
	@Override
	//FOR SORT DECENDING K1.compareto.k2 ==> k2.compareto.k1 
	public int compare(WritableComparable w1, WritableComparable w2) {
		SysNameWithNumericKey k1=(SysNameWithNumericKey) w1;
		SysNameWithNumericKey k2=(SysNameWithNumericKey) w2;
		int result=k2.getSysName().compareTo(k1.getSysName());
		if(0==result) {
			result=k2.getCpuAvg().compareTo(k1.getCpuAvg());
		}
		return result;
	}
   
}
