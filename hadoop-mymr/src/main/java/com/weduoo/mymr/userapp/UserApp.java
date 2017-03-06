package com.weduoo.mymr.userapp;

import java.util.HashMap;

import com.weduoo.mymr.common.Constants;
import com.weduoo.mymr.scheduler.Runner;

public class UserApp {

	static final String BASPATH="/Users/weduoo/test/hadoop/task/";
	public static void main(String[] args) throws Exception {
		
		//设置用户的配置文件
		HashMap<String, String> conf = new HashMap<String, String>();
		conf.put(Constants.INPUT_PATH, BASPATH+"a.txt");
		conf.put(Constants.OUTPUT_PATH, BASPATH+"out.txt");
		conf.put(Constants.INPUT_FORMAT, "com.weduoo.mymr.io.DefaultInputFormat");
		conf.put(Constants.OUTPUT_FORMAT, "com.weduoo.mymr.io.DefaultOutPutFormat");
		conf.put(Constants.JAR_PATH, BASPATH+"test.jar");
		conf.put(Constants.WORKER_HOST, "localhost");
		conf.put(Constants.WORKER_PORT, "9889");
		conf.put(Constants.USER_LOGIC, "com.weduoo.mymr.userapp.UserLogic");

		Runner runner = new Runner(conf);
		runner.submit("localhost", 9889);

	}

}
