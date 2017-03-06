package com.weduoo.mymr.io;

import java.io.BufferedReader;
import java.io.FileReader;

import com.weduoo.mymr.common.Constants;
import com.weduoo.mymr.common.Context;


public class DefaultInputFormat extends InputFormat {

	private String inputPath;
	private BufferedReader br = null;
	private int key;
	private String value;
	private int lineNumber = 0;

	public void init(Context context) throws Exception {
		this.inputPath = context.getConfiguration().get(Constants.INPUT_PATH);
		this.br = new BufferedReader(new FileReader(inputPath));

	}

	@Override
	public int nextKey() {

		return this.key;
	}

	@Override
	public String nextValue() {

		return this.value;
	}

	@Override
	public boolean hasNext() throws Exception {

		String line = null;
		line = readLine();
		this.key = lineNumber++;
		this.value = line;

		return null != line;

	}

	@Override
	public String readLine() throws Exception {
		String line = br.readLine();
		if (line == null)
			br.close();
		return line;
	}


}
