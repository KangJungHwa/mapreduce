package com.stis.irs.irs_system;

import org.apache.hadoop.util.ProgramDriver;

public class TotalDriver {
  public static void main(String[] args) {
	    int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    try {
	    	pgd.addClass("irsmax", IrsMaxDriver.class, 
	                   "A map/reduce program that cpumax & cpuavg in the input files.");
	    	pgd.addClass("EqpUsageDriver", EqpUsageDriver.class, 
	                   "A map/reduce program that join with Equipment Data & Log Data");
	    	exitCode = pgd.run(args);
	    	
		} catch (Throwable e) {
			e.printStackTrace();
		}
	    System.exit(exitCode);
}
}
