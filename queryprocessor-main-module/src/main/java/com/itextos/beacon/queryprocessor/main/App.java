package com.itextos.beacon.queryprocessor.main;

public class App {

	public static void main(String[] args) {
		
		
		String module=System.getenv("module");
		System.out.println("module : "+module);

		if(module==null) {
			module="";
		}

		if(module.equals("qpr")) {
			com.itextos.beacon.queryprocessor.threadpoolexecutor.ProcessQueueThreadPool.main(args);

			com.itextos.beacon.queryprocessor.requestreceiver.QueryEngine.main(args);

			
		}else if(module.equals("qptp")) {
			
			com.itextos.beacon.queryprocessor.threadpoolexecutor.ProcessQueueThreadPool.main(args);
		}
	}

}
