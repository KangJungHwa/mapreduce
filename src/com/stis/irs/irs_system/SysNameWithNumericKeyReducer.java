package com.stis.irs.irs_system;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SysNameWithNumericKeyReducer extends Reducer<SysNameWithNumericKey, Text, SysNameWithNumericKey, Text> {
    private SysNameWithNumericKey sysKey=new SysNameWithNumericKey();
	@Override
	protected void reduce(SysNameWithNumericKey key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		int seq=0;
       
		for (Text value : values) {
		    
			sysKey.setSysName(key.getSysName());
			sysKey.setCpuAvg(key.getCpuAvg());
			context.write(sysKey, value);
			seq++;
			if(seq==3) {
				seq=0;
		    	break;
		    }
			
			
		}
	}

	
}
