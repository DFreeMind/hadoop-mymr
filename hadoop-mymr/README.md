#待处理事情

##简单需求
1.	索引创建
2.	共同好友
3.	商品JOIN订单操作
4.	订单的TOPN（Groupingcomparaor、CompareTo）
5.	移动日志（自定义OutputFormat、setup方法）

##中型项目【点击流日志分析】
1.	点击流日志分析项目

	a 点击流模型（ROI 投资回报率）

2.	流量分析常见指标
	
	1.	基础分析
	2.	来源分析
	3.	受访分析
	4.	访客分析
	5.	转化路径分析
	
3.	实现步骤

	1.	数据清洗
	2.	用户识别（PV数据模型，PV结果数据汇总）
	3.	自动调度（azkaban）脚本开发——自动数据清洗
	4.	hive中创建表（星型模型/雪花型模型）
	


#Hadoop提供的功能

1.	JOIN连接，Cache缓存





#常用问题

1.	如何从外部想MapReduce传入参数（配置文件、conf.set(name,value)）
2.	日志埋点