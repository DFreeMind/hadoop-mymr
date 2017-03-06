package com.weduoo.mymr.scheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

import com.weduoo.mymr.common.Constants;

public class WorkerRunnable implements Runnable {
	Socket socket;
	InputStream in = null;
	volatile long confSize = 0;
	volatile long jarSize = 0;

	public WorkerRunnable(Socket socket) {
		this.socket = socket;
	}

	public void run() {
		try {
			this.in = socket.getInputStream();
			byte[] protocal = new byte[7];
			int read = in.read(protocal, 0, 7);
			if (read < 7) {
				System.out.println("客户端请求不符合协议规范......");
				return;
			}
			String command = new String(protocal);
			switch (command) {
			case "jarfile":
				receiveJarFile();
				break;
			case "jobconf":
				receiveConfFile();
				break;
			case "job2run":
				runJob();
				break;
			default:
				System.out.println("客户端请求不符合协议规范.....");
				socket.close();
				break;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	/***
	 * 接受配置文件
	 * @throws Exception
	 */
	private void receiveConfFile() throws Exception {
		System.out.println("开始接收conf文件");
		FileOutputStream fo = new FileOutputStream(Constants.TASK_WORK_DIR + "/" + Constants.CONF_FILE);
		byte[] buff = new byte[4096];
		int read = 0;
		while ((read = in.read(buff)) != -1) {
			confSize += read;
			fo.write(buff, 0, read);
		}
		fo.flush();
		fo.close();
		in.close();
		socket.close();

	}
	/**
	 * 接受jar文件
	 * @throws Exception
	 */
	private void receiveJarFile() throws Exception {
		System.out.println("开始接收jar文件");
		FileOutputStream fo = new FileOutputStream(Constants.TASK_WORK_DIR + "/" + Constants.JAR_FILE);
		byte[] buff = new byte[4096];
		int read = 0;
		while ((read = in.read(buff)) != -1) {
			jarSize += read;
			fo.write(buff, 0, read);
		}
		fo.flush();
		fo.close();
		in.close();
		socket.close();

	}
	/**
	 * 启动任务
	 * @throws Exception
	 */
	private void runJob() throws Exception {

		byte[] buff = new byte[4096];
		int read = in.read(buff);
		String shell = new String(buff, 0, read);
		System.out.println("接收到启动命令......." + shell);
		in.close();
		socket.close();
		Thread.sleep(500);

		File jarFile = new File(Constants.TASK_WORK_DIR + "/" + Constants.JAR_FILE);
		File confFile = new File(Constants.TASK_WORK_DIR + "/" + Constants.CONF_FILE);
		System.out.println("jarfile 存在？" + jarFile.exists());
		System.out.println("confFile 存在？" + confFile.exists());
		System.out.println("jarfile可读？" + jarFile.canRead());
		System.out.println("jarfile可写？" + jarFile.canWrite());
		System.out.println("confFile可读？" + confFile.canRead());
		System.out.println("confFile可写？" + confFile.canWrite());

		System.out.println("jarFile.length():" + jarFile.length());
		System.out.println("confFile.length():" + confFile.length());

		/*if (jarFile.length() == jarSize && confFile.length() == confSize) {
			System.out.println("jar 和 conf 文件已经准备就绪......");
		}*/
		System.out.println("开始启动数据处理TaskProcessor......");

		Process exec = Runtime.getRuntime().exec(shell);
		//等待运行的完成
		int waitFor = exec.waitFor();
		
		InputStream errStream = exec.getErrorStream();
		BufferedReader errReader = new BufferedReader(new InputStreamReader(errStream));
		String inLine = null;
		/*
		 * InputStream stdStream = exec.getInputStream(); BufferedReader
		 * stdReader = new BufferedReader(new InputStreamReader(stdStream));
		 * while ((inLine = stdReader.readLine()) != null) {
		 * System.out.println(inLine); }
		 */
		while ((inLine = errReader.readLine()) != null) {
			System.out.println(inLine);
		}
		
		if (waitFor == 0) {
			System.out.println("task成功运行完毕.....");
		} else {
			System.out.println("task异常退出......");
		}

	}

}
