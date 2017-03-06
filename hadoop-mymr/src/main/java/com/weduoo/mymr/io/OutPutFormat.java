package com.weduoo.mymr.io;

import com.weduoo.mymr.common.Context;

public abstract class OutPutFormat {
	
	public abstract void write(Context context) throws Exception;

	public abstract void cleanUp() throws Exception ;

}
