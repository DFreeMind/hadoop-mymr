package com.weduoo.mymr.io;

import com.weduoo.mymr.common.Context;

public abstract class InputFormat {
	
	
	public abstract int nextKey();

	public abstract String nextValue();

	public abstract String readLine() throws Exception;
	
	public abstract boolean hasNext() throws Exception;
	
	public abstract void init(Context context) throws Exception;
	
}
