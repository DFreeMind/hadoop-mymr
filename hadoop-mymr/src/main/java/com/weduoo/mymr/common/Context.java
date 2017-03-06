package com.weduoo.mymr.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
/**
 * 全局上下文
 * @author weduoo
 *
 */
public class Context {

	private HashMap<String, Integer> KVBuffer = new HashMap<String, Integer>();
	private HashMap<String, String> conf;

	@SuppressWarnings("unchecked")
	public Context() throws Exception {
		// 加载配置参数
		File file = new File(Constants.TASK_WORK_DIR + "/" + Constants.CONF_FILE);
		if (file.exists()) {
			ObjectInputStream oi = new ObjectInputStream(new FileInputStream(file));
			this.conf = (HashMap<String, String>) oi.readObject();
		} else {
			// throw new RuntimeException("read conf failed .......");
		}
	}

	public void write(String k, Integer v) {
		KVBuffer.put(k, v);
	}

	public HashMap<String, Integer> getKVBuffer() {
		return KVBuffer;
	}

	public void setKVBuffer(HashMap<String, Integer> tmpKV) {
		this.KVBuffer = tmpKV;
	}

	public HashMap<String, String> getConfiguration() {
		return conf;
	}

	public void setConfiguration(HashMap<String, String> configuration) {
		this.conf = configuration;
	}

}
