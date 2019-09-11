package logparser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class AvgMaxTuple implements Writable {
	private float cpumax = 0;
	private float cpuavg = 0;
	private float memmax = 0;
	private float memavg = 0;
	private long count=0;
	private String EVENT_TIME=null;
	private String equipKey=null;
	
	
	public String getEVENT_TIME() {
		return EVENT_TIME;
	}

	public void setEVENT_TIME(String EVENT_TIME) {
		this.EVENT_TIME = EVENT_TIME;
	}
	
	public String getEquipKey() {
		return equipKey;
	}

	public void setEquipKey(String equipKey) {
		this.equipKey = equipKey;
	}
	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
	
	
	public float getCpumax() {
		return cpumax;
	}

	public void setCpumax(float cpumax) {
		this.cpumax = cpumax;
	}

	public float getCpuavg() {
		return cpuavg;
	}

	public void setCpuavg(float cpuavg) {
		this.cpuavg = cpuavg;
	}

	public float getMemmax() {
		return memmax;
	}

	public void setMemmax(float memmax) {
		this.memmax = memmax;
	}

	public float getMemavg() {
		return memavg;
	}

	public void setMemavg(float memavg) {
		this.memavg = memavg;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		equipKey =WritableUtils.readString(in);
		EVENT_TIME =WritableUtils.readString(in);
		cpumax = in.readFloat();
		cpuavg = in.readFloat();
		memmax = in.readFloat();
		memavg = in.readFloat();
		count = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, equipKey);
		WritableUtils.writeString(out, EVENT_TIME);
		out.writeFloat(cpumax);
		out.writeFloat(cpuavg);
		out.writeFloat(memmax);
		out.writeFloat(memavg);		
		out.writeLong(count);	
	}

	@Override
	public String toString() {
		return equipKey +","+EVENT_TIME+","+ String.valueOf(cpumax)+","+String.valueOf(cpuavg)+","+String.valueOf(memmax)+","+String.valueOf(memavg);
	}
}
