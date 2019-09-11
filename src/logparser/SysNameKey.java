package logparser;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class SysNameKey implements WritableComparable<SysNameKey>{
    private String sysname;
    
	public SysNameKey() {
	}

	public String getSysanme() {
		return sysname;
	}

	public void setSysanme(String sysanme) {
		this.sysname = sysanme;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sysname=WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, sysname);
	}

	@Override
	public int compareTo(SysNameKey key) {
		int result=sysname.compareTo(key.sysname);
		return result;
	}

}
