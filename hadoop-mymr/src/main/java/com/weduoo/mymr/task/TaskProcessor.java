package com.weduoo.mymr.task;

import java.util.HashMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.weduoo.mymr.common.Constants;
import com.weduoo.mymr.common.Context;
import com.weduoo.mymr.io.InputFormat;
import com.weduoo.mymr.io.OutPutFormat;

public class TaskProcessor {
	
	public static void main(String[] args) throws Exception {
		// 加载用户指定的所有配置参数到上下文对象中
		Context context = new Context();
		HashMap<String, String> conf = context.getConfiguration();
		
		Logger logger = Logger.getLogger("TaskProcessor");
		logger.setLevel(Level.INFO);
		FileHandler fileHandler = new FileHandler("/Users/weduoo/test/hadoop/task/task.log");
		fileHandler.setLevel(Level.INFO);
		logger.addHandler(fileHandler);
		logger.info("context: " + context);
		logger.info("conf: " + conf);
		/*FileOutputStream log = new FileOutputStream("d:/task/task.log");
		log.write(("context: " + context+"\r").getBytes());
		log.write(("conf: " + conf+"\r").getBytes());
		log.close();*/
		
		// 初始化文件读取组件
		Class<?> forName = Class.forName(conf.get(Constants.INPUT_FORMAT));
		InputFormat inputFormat = (InputFormat) forName.newInstance();
		inputFormat.init(context);
		
		// 用inputformat组件读数据，并调用用户逻辑
		Class<?> forName2 = Class.forName(conf.get(Constants.USER_LOGIC));
		ProcessLogic userLogic = (ProcessLogic) forName2.newInstance();
		// 对每一行数据调用一次用户逻辑，并通过context将用户调用结果存入内部缓存
		while (inputFormat.hasNext()) {
			Integer key = inputFormat.nextKey();
			String value = inputFormat.nextValue();
			userLogic.process(key,value,context);
			
		}
		userLogic.cleanUp(context);
		
		// 替用户输出结果
		Class<?> forName3 = Class.forName(conf.get(Constants.OUTPUT_FORMAT));
		OutPutFormat outputFormat = (OutPutFormat) forName3.newInstance();
		
		outputFormat.write(context);
		
	}
	
	
	

}
