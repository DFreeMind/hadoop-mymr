package com.weduoo.mymr.scheduler;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;

import com.weduoo.mymr.common.Constants;

public class WorkerClient {

	private HashMap<String, String> conf;
	Socket socket =null;
	OutputStream so = null;

	public WorkerClient(HashMap<String, String> conf) {
		this.conf = conf;
	}

	// 提交job
	public void submit() throws Exception {

		socket = new Socket(conf.get(Constants.WORKER_HOST), Integer.parseInt(conf.get(Constants.WORKER_PORT)));
		so = socket.getOutputStream();

		String jarPath = conf.get(Constants.JAR_PATH);

		// 发送jar包
		byte[] buff = new byte[4096];
		FileInputStream jarIns = new FileInputStream(jarPath);
		so.write("jarfile".getBytes());
		int read = 0;
		while ((read=jarIns.read(buff)) != -1) {
			so.write(buff,0,read);
		}
		jarIns.close();
		so.close();
		socket.close();
		
		
		
		// 发送job.conf文件
		socket = new Socket(conf.get(Constants.WORKER_HOST), Integer.parseInt(conf.get(Constants.WORKER_PORT)));
		so = socket.getOutputStream();
		
		FileInputStream confIns = new FileInputStream(Constants.CONF_FILE);
		so.write("jobconf".getBytes());
		while ((read = confIns.read(buff)) != -1) {
			so.write(buff,0,read);
		}
		confIns.close();
		so.close();
		socket.close();

		// 发送启动命令
		socket = new Socket(conf.get(Constants.WORKER_HOST), Integer.parseInt(conf.get(Constants.WORKER_PORT)));
		so = socket.getOutputStream();
		so.write("job2run".getBytes());
		String shell = "java -cp /Users/weduoo/test/hadoop/task/job.jar com.weduoo.mymr.task.TaskProcessor";
		so.write(shell.getBytes());
		so.close();
		socket.close();
	}

}
