package com.stis.irs.irs_system;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SysNameWithNumericKey implements WritableComparable<SysNameWithNumericKey> {
   private Integer sysName;
   private Float cpuAvg ;
   
public SysNameWithNumericKey() {

}
public SysNameWithNumericKey(Integer sysName, Float cpuAvg) {
	super();
	this.sysName = sysName;
	this.cpuAvg = cpuAvg;
}
public Integer getSysName() {
	return sysName;
}
public void setSysName(Integer sysName) {
	this.sysName = sysName;
}
public Float getCpuAvg() {
	return cpuAvg;
}
public void setCpuAvg(Float cpuAvg) {
	this.cpuAvg = cpuAvg;
}
@Override
public void readFields(DataInput in) throws IOException {
	sysName=in.readInt();
	cpuAvg=in.readFloat();
	
}
@Override
public void write(DataOutput out) throws IOException {
	out.writeInt(sysName);
	out.writeFloat(cpuAvg);
}
@Override
public int compareTo(SysNameWithNumericKey key) {
	int result=sysName.compareTo(key.sysName);
	if(0==result) {
		result=cpuAvg.compareTo(key.cpuAvg);
	}
	return 0;
}
@Override
public String toString() {
	return (new StringBuilder().append(sysName).append(",").append(cpuAvg)).toString();
}
   
}
