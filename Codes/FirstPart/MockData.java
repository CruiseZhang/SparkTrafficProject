package com.spark.spark.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.bjsxt.spark.util.DateUtils;
import com.bjsxt.spark.util.StringUtils;


/**
 * 模拟数据  数据格式如下：
 * 
 * 日期	 卡口ID		 摄像头编号  车牌号	拍摄时间	        车速	    道路ID   	区域ID
 * date	 monitor_id	 camera_id	 car	action_time		speed	road_id		area_id
 * 
 * monitor_flow_action
 * monitor_camera_info
 * 
 * @author Administrator
 */
public class MockData {
    public static void mock(JavaSparkContext sc, SparkSession spark) {
    	//https://blog.csdn.net/Barcon/article/details/82628120
    	List<Row> dataList = new ArrayList<>();
    	Random random = new Random();

    	String[] locations = new String[]{"鲁","川","渝","京","沪","贵","云","深","浙","粤"};  // 10个，所以对应后面bound:10
//     	String[] areas = new String[]{"海淀区","朝阳区","昌平区","东城区","西城区","丰台区","顺义区","大兴区"};
    	//date: 如2019-12-01
    	String date = DateUtils.getTodayDate(); // 获取当前时间
    	
    	/**
    	 * 模拟3000个车辆
    	 */
    	for (int i = 0; i < 3000; i++) {
    		//模拟车牌号：如：京A00001，bound: 10是0到9。65+0到25的随机值再转成字符就是大写字母A到Z。模拟5位数的车牌号。
        	String car = locations[random.nextInt(10)] + (char)(65+random.nextInt(26))+StringUtils.fulfuill(5,random.nextInt(100000)+"");
        	
        	//baseActionTime 模拟24小时，即0到23时。date格式是2019-12-01
			// 值得一提的是此处fulfuill在StringUtils类有两个同名方法。一个是默认补全2成两位数；一个是任意自定义位数。
        	String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24)+""); //2018-01-01 01
        	/**
        	 * 这里的for循环模拟每辆车经过不同的卡扣不同的摄像头数据。
        	 */
        	// +1是因为万一模拟出来是0，可以确保至少循环1次而不是0次。
        	for(int j = 0 ; j < (random.nextInt(300) + 1); j++){
        		//模拟每个车辆每被30个摄像头拍摄后时间上累计加1小时。这样做使数据更真实。
        		if(j % 30 == 0 && j != 0){
        			//2018-01-01 01
        			 baseActionTime = date + " " + StringUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")[1])+1)+"");
        		}
				//模拟areaId 【一共8个区域】且必须是01到08这种格式，不是1到8
        		String areaId = StringUtils.fulfuill(2,random.nextInt(8)+1+"");
				//模拟道路id 【1~50 个道路】
        		String roadId = random.nextInt(50)+1+"";
				//模拟9个卡扣monitorId，0补全4位
        		String monitorId = StringUtils.fulfuill(4, random.nextInt(9)+"");
				//模拟摄像头id cameraId
        		String cameraId = StringUtils.fulfuill(5, random.nextInt(100000)+"");

        		String actionTime = baseActionTime + ":"  // 2018-01-01 20:
        				+ StringUtils.fulfuill(random.nextInt(60)+"") + ":" //模拟分钟，即09:
        				+ StringUtils.fulfuill(random.nextInt(60)+"");//模拟经过此卡扣开始时间 ，如：2018-01-01 20:09:10
        		String speed = (random.nextInt(260)+1)+"";//模拟速度
        		//RowFactory是创建rdd对象的类
        		Row row = RowFactory.create(date,monitorId,cameraId,car,actionTime,speed,roadId,areaId);
        		dataList.add(row);
        	}
		}
    	
    	/**
    	 * 2018-4-20 1	22	京A1234 
    	 * 2018-4-20 1	23	京A1234 
    	 * 1 【22,23】
    	 * 1 【22,23,24】
    	 */

    	// parallelize在一个已经存在的集合上创建出一个可以被并行操作的分布式数据集即rdd
		// https://www.jianshu.com/p/c688b8856dd8
    	JavaRDD<Row> rowRdd = sc.parallelize(dataList);

    	// https://www.cnblogs.com/wei-jing/p/10540192.html
		// https://www.cnblogs.com/alomsc/p/10996223.html
    	StructType cameraFlowSchema = DataTypes.createStructType(Arrays.asList(
    			DataTypes.createStructField("date", DataTypes.StringType, true),
    			DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
    			DataTypes.createStructField("camera_id", DataTypes.StringType, true),
    			DataTypes.createStructField("car", DataTypes.StringType, true),
    			DataTypes.createStructField("action_time", DataTypes.StringType, true),
    			DataTypes.createStructField("speed", DataTypes.StringType, true),
    			DataTypes.createStructField("road_id", DataTypes.StringType, true),
    			DataTypes.createStructField("area_id", DataTypes.StringType, true)
    			));

		//Java里是Dateset，Scala里是DataFrame
    	Dataset<Row> ds = spark.createDataFrame(rowRdd, cameraFlowSchema);
    	
    	//默认打印出来df里面的20行数据
    	System.out.println("----打印 车辆信息数据----");
		ds.show();
		//spark1.6写成registerTempTable
		//spark2.0写成createTempView
		ds.registerTempTable("monitor_flow_action"); //注册一张临时表，字段即为98-105行
 
    	/**
    	 * monitorAndCameras    key：monitor_id
    	 * 						value:hashSet(camera_id)
    	 * 基于生成的数据，生成对应的卡扣号和摄像头对应基本表
    	 */
    	// String是key，放卡口号；后面Set是value，有自动去重功能，放摄像头编号；
		// 如：2018-11-05	0004	42392	京E14566	2018-11-05 03:34:59	115	2	03
		// key放0004，Set放42392
		// https://www.runoob.com/java/java-map-interface.html
    	Map<String,Set<String>> monitorAndCameras = new HashMap<>();
    	int index = 0;
    	for(Row row : dataList){ // 经过遍历值后，最终会得到九个卡扣（0000-0009）以及每个卡扣下对应的数个摄像头
    		//row.getString(1)即monitor_id
    		Set<String> sets = monitorAndCameras.get(row.getString(1)); // get方法返回key对应的value
    		if(sets == null){  // 如果一开始key没有对应任何value
    			sets = new HashSet<>();
    			monitorAndCameras.put((String)row.getString(1), sets); // 本来是object类型，转成String类型
    		}
    		//这里每隔1000条数据随机插入一条摄像头编号，模拟出来标准表中卡扣对应摄像头的数据比模拟数据中多出来的摄像头。
			//这个摄像头的数据不一定会在车辆数据中有。即可以看出卡扣号下有坏的摄像头。
    		index++;
    		if(index % 1000 == 0){
    			sets.add(StringUtils.fulfuill(5, random.nextInt(100000)+""));
    		} 
    		//row.getString(2)是camera_id
			String cameraId = row.getString(2);
			sets.add(cameraId);
    	}
    	// https://blog.csdn.net/sinat_39308893/article/details/86489346
    	dataList.clear(); // 只是清除了对象的引用，对象仍在

    	// https://www.cnblogs.com/dreammyone/articles/9960400.html
		//返回映射所包含的映射关系的Set集合（一个关系就是一个键-值对），就是把(key-value)作为一个整体一对一对地存放到Set集合当中的。
    	Set<Entry<String,Set<String>>> entrySet = monitorAndCameras.entrySet();
    	for (Entry<String, Set<String>> entry : entrySet) {
    		String monitor_id = entry.getKey();
    		Set<String> sets = entry.getValue();
    		Row row = null;
    		for (String camera_id : sets) {
    			row = RowFactory.create(monitor_id,camera_id);
    			dataList.add(row);
			}
		}

		// 动态创建Schema
    	StructType monitorSchema = DataTypes.createStructType(Arrays.asList(
	    			DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
	    			DataTypes.createStructField("camera_id", DataTypes.StringType, true)
    			));
    	

    	rowRdd = sc.parallelize(dataList);
		// https://www.cnblogs.com/LHWorldBlog/p/8431634.html
    	Dataset<Row> monitorDF = spark.createDataFrame(rowRdd, monitorSchema);
    	monitorDF.registerTempTable("monitor_camera_info");
    	System.out.println("----打印 卡扣号对应摄像头号 数据----");
    	monitorDF.show();
    }
}
