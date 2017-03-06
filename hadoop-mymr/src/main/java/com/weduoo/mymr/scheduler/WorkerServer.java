package com.weduoo.mymr.scheduler;

import java.net.ServerSocket;
import java.net.Socket;

public class WorkerServer {

	// 接收请求
	public static void main(String[] args) throws Exception {
		ServerSocket ssc = new ServerSocket(9889);
		System.out.println("Worker服务器启动-->9889");
		while (true) {
			Socket accept = ssc.accept();
			//启动一个新线程
			new Thread(new WorkerRunnable(accept)).start();
		}
	}

}
