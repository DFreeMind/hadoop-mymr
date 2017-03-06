package com.weduoo.mymr.io;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import com.weduoo.mymr.common.Constants;
import com.weduoo.mymr.common.Context;


public class DefaultOutPutFormat extends OutPutFormat {
	BufferedWriter bw =null;

	@Override
	public void write(Context context) throws Exception {

		String outputPath = context.getConfiguration().get(Constants.OUTPUT_PATH);
		HashMap<String, Integer> KVBuffer = context.getKVBuffer();
		//将输入放入输出目录中
		this.bw = new BufferedWriter(new FileWriter(outputPath));
		Set<Entry<String, Integer>> entrySet = KVBuffer.entrySet();
		for (Entry<String, Integer> ent : entrySet) {
			bw.write(ent.getKey() + "\t" + ent.getValue()+"\r");
		}
		bw.flush();
		

	}

	@Override
	public void cleanUp() throws Exception{
		bw.close();
	}
	
}
