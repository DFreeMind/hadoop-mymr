package com.weduoo.mapreduce.flow_analyse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

/**
 * 加载数据库中的元数据
 * @author weduoo
 *
 */
public class DBloader {
	//从知识库中加载数据到map，在MapReduce处理数据时进行匹配
	public static void loadDB(HashMap<String, String> ruleMap) throws Exception{
		Connection conn = null;
		Statement st = null;
		ResultSet res = null;
		try {
			//连接数据库并做查询
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/weduoo","root","1234");
			st = conn.createStatement();
			res = st.executeQuery("select url,content from url_rule");
			while(res.next()){
				ruleMap.put(res.getString(1), res.getString(2));
			}
		} finally {
			try {
				if(res!=null){
					res.close();
				}
				if(st!=null){
					st.close();
				}
				if(conn!=null){
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
