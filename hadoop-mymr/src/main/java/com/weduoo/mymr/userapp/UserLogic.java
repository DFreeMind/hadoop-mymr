package com.weduoo.mymr.userapp;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import com.weduoo.mymr.common.Context;
import com.weduoo.mymr.task.ProcessLogic;

public class UserLogic extends ProcessLogic {
	private HashMap<String, Integer> wordCount = new HashMap<String, Integer>();

	@Override
	public void process(Integer k, String v, Context context) {
		String[] words = v.split(" ");
		for (String word : words) {
			Integer count = wordCount.get(word);
			if (count == null) {
				wordCount.put(word, 1);
			} else {
				wordCount.put(word, count + 1);
			}
		}
	}

	@Override
	public void cleanUp(Context context) {
		//将结果输出
		Set<Entry<String, Integer>> entrySet = wordCount.entrySet();
		for (Entry<String, Integer> ent : entrySet) {
			context.write(ent.getKey(), ent.getValue());
		}
	}

}
