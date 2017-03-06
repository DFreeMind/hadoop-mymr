package com.weduoo.mymr.scheduler;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import com.weduoo.mymr.common.Constants;


public class Runner {
	private HashMap<String, String> conf;

	public Runner(HashMap<String, String> conf) {

		this.conf = conf;

	}

	public void submit(String host,int port) throws Exception {
		ObjectOutputStream jobConfStream = new ObjectOutputStream(new FileOutputStream(Constants.CONF_FILE));
		jobConfStream.writeObject(conf);
		
		WorkerClient workerClient = new WorkerClient(conf);
		workerClient.submit();
		
	}

}
