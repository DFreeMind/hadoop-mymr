package com.weduoo.mymr.task;

import com.weduoo.mymr.common.Context;

public abstract class ProcessLogic{

	public abstract void process(Integer k, String v, Context context);
	
	
	public void cleanUp(Context context){};

}
