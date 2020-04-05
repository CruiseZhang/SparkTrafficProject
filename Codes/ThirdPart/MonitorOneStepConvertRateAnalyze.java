package com.bjsxt.spark.areaRoadFlow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.bjsxt.spark.conf.ConfigurationManager;
import com.spark.spark.test.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.bjsxt.spark.constant.Constants;
import com.bjsxt.spark.dao.ITaskDAO;
import com.bjsxt.spark.dao.factory.DAOFactory;
import com.bjsxt.spark.domain.Task;
import com.bjsxt.spark.util.DateUtils;
import com.bjsxt.spark.util.NumberUtils;
import com.bjsxt.spark.util.ParamUtils;
import com.bjsxt.spark.util.SparkUtils;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * 功能9——指定卡扣流量转化率
 * 指定一个道路流如0001 0002 0003 0004
 * 若有<0001,carCount> <0001,0002, carCount2> 转化率则为——carCount2/carCount1
 * 求0001 0002 0003的转化率       0001 0002 0003的车流量/0001 0002的车流量
 * 轨迹——(car,row)——京A1234	0001,0002,0003,0006,0002,0003
 * 1、查询出来的数据封装到cameraRDD
 * 2、计算每一车的轨迹  
 * 3、匹配指定的道路流       0001：carCount   0001，0002：carCount   0001,0002,0003 carCount
 * @author root
 */
public class MonitorOneStepConvertRateAnalyze {
	public static void main(String[] args) {

		/**
		 * 判断应用程序是否在本地执行
		 */
		JavaSparkContext sc = null;
		SparkSession spark = null;
		Boolean onLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

		if(onLocal){
			// 构建Spark运行时的环境参数
			SparkConf conf = new SparkConf()
					.setAppName(Constants.SPARK_APP_NAME)
//			.set("spark.sql.shuffle.partitions", "300")
//			.set("spark.default.parallelism", "100")
//			.set("spark.storage.memoryFraction", "0.5")
//			.set("spark.shuffle.consolidateFiles", "true")
//			.set("spark.shuffle.file.buffer", "64")
//			.set("spark.shuffle.memoryFraction", "0.3")
//			.set("spark.reducer.maxSizeInFlight", "96")
//			.set("spark.shuffle.io.maxRetries", "60")
//			.set("spark.shuffle.io.retryWait", "60")
//			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//			.registerKryoClasses(new Class[]{SpeedSortKey.class})
					;
			/**
			 * 设置spark运行时的master  根据配置文件来决定的
			 */
			conf.setMaster("local");
			sc = new JavaSparkContext(conf);

			spark = SparkSession.builder().getOrCreate();
			/**
			 * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的表就可以
			 * 本地模拟数据注册成一张临时表
			 * monitor_flow_action	数据表：监控车流量所有数据
			 * monitor_camera_info	标准表：卡扣对应摄像头标准表
			 */
			MockData.mock(sc, spark);
		}else{
			System.out.println("++++++++++++++++++++++++++++++++++++++开启hive的支持");
			/**
			 * "SELECT * FROM table1 join table2 ON (连接条件)"  如果某一个表小于20G 他会自动广播出去
			 * 会将小于spark.sql.autoBroadcastJoinThreshold值（默认为10M）的表广播到executor节点，不走shuffle过程,更加高效。
			 *
			 * config("spark.sql.autoBroadcastJoinThreshold", "1048576000");  //单位：字节
			 */
			spark = SparkSession.builder().config("spark.sql.autoBroadcastJoinThreshold", "1048576000").enableHiveSupport().getOrCreate();
			spark.sql("use traffic");
		}

//			// 1、构造Spark上下文
//			SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME);
//			SparkUtils.setMaster(conf);
//
//			JavaSparkContext sc = new JavaSparkContext(conf);
//			SparkSession spark = SparkUtils.getSQLContext(sc);
//
//			// 2、生成模拟数据
//			SparkUtils.mockData(sc, spark);
			
			// 3、查询任务，获取任务的参数为5
			long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT);//参数为5
			
			ITaskDAO taskDAO = DAOFactory.getTaskDAO();
			Task task = taskDAO.findTaskById(taskid);//taskid为5，即去mysql中查找taskid是5的task参数
			if(task == null) {
				return;
			}
			// 拿出数据库中的json格式字段并转成json对象
			JSONObject taskParam = JSONObject.parseObject(task.getTaskParams());//taskid为5查出来的字段是json格式，先转化为json对象
			
			/**
			 * 从数据库中查找出指定的卡扣流
			 * 0001,0002,0003,0004,0005
			 */
			//拿出myql数据表taskid为5对应的字段里的roadFlow，即0001,0002,0003,0004,0005
			String roadFlow = ParamUtils.getParam(taskParam, Constants.PARAM_MONITOR_FLOW);
			// 就1个字符串，其实广播与否对内存大小影响不大
			final Broadcast<String> roadFlowBroadcast = sc.broadcast(roadFlow);
			
			/**
			 * 拿到指定日期下合法的车辆，该方法返回的是行数据集合，单条如下
			 * 2019-12-17 0000 56185 川R86130 2019-12-17 14:40:14 81 31	02
			 */
			JavaRDD<Row> rowRDDByDateRange = SparkUtils.getCameraRDDByDateRange(spark, taskParam);

			/**
			 * 将rowRDDByDateRange变成key-value对的形式，key为car，value为详细信息
			 * （key,row）
			 * 为什么要变成kv对的形式？
			 * 因为下面要对car按照时间排序，绘制出这辆车的轨迹。
			 * 原始row——2019-12-17	0000 56185 川R86130	2019-12-17 14:40:14	81	31	02
			 */
			// 得到row——川R86130, 2019-12-17 0000	56185 川R86130	2019-12-17 14:40:14	 81	31 02
			JavaPairRDD<String, Row> car2RowRDD = getCar2RowRDD(rowRDDByDateRange);// 图里初始row转car2RowRDD的过程


			/**
			 * 计算一辆车有多少次匹配到指定的卡扣流
			 * 先拿到车辆的轨迹，比如一辆车轨迹：0001,0002,0003,0004,0001,0002,0003,0001,0004
			 * 返回一个二元组（切分片段，该片段对应该车辆轨迹中匹配上的次数）
			 * ("0001",3)
			 * ("0001,0002",2)
			 * ("0001,0002,0003",2)
			 * ("0001,0002,0003,0004",1)
			 * ("0001,0002,0003,0004,0005",0)
			 * ... ... 
			 * ("0001",13)
			 * ("0001,0002",12)
			 * ("0001,0002,0003",11)
			 * ("0001,0002,0003,0004",11)
			 * ("0001,0002,0003,0004,0005",10)
			 */
			JavaPairRDD<String, Long> roadSplitRDD = generateAndMatchRowSplit(taskParam,roadFlowBroadcast,car2RowRDD);
			
			/**
			 * roadSplitRDD
			 * 所有的相同的key先聚合得到总数
			 * ("0001",100)
			 * ("0001,0002",200)
			 * ("0001,0002,0003",300)
			 * ("0001,0002,0003,0004",400)
			 * ("0001,0002,0003,0004,0005",500) 
			 * 变成了一个 K,V格式的map
			 * ("0001",500)
			 * ("0001,0002",400)
			 * ("0001,0002,0003",300)
			 * ("0001,0002,0003,0004",200)
			 * ("0001,0002,0003,0004,0005",100) 
			 */
			Map<String, Long> roadFlow2Count = getRoadFlowCount(roadSplitRDD);
			
			Map<String, Double> convertRateMap = computeRoadSplitConvertRate(roadFlow,roadFlow2Count);
			
			for (Entry<String, Double> entry : convertRateMap.entrySet()) {
				System.out.println(entry.getKey()+"="+entry.getValue());
			}
	}
	private static JavaPairRDD<String, Row> getCar2RowRDD(JavaRDD<Row> car2RowRDD) {
		return car2RowRDD.mapToPair(new PairFunction<Row, String, Row>() {

			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(3), row); //车牌号和row
			}
		});
	}

	private static JavaPairRDD<String, Long> generateAndMatchRowSplit(JSONObject taskParam,
		  			final Broadcast<String> roadFlowBroadcast,JavaPairRDD<String, Row> car2RowRDD) {
		return car2RowRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Long>() {
			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Long>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				String car = tuple._1;
				Iterator<Row> iterator = tuple._2.iterator();
				List<Tuple2<String, Long>> list = new ArrayList<>();

				List<Row> rows = new ArrayList<>();
				/**
				 * 遍历这一辆车详细信息，然后将详细信息放入到rows集合中
				 */
				while(iterator.hasNext()){
					Row row = iterator.next();
					rows.add(row);
				}

				/**
				 * 对这个rows集合按照车辆通过卡扣的时间排序
				 */
				Collections.sort(rows, new Comparator<Row>() {
					@Override
					public int compare(Row row1, Row row2) {
						String actionTime1 = row1.getString(4);
						String actionTime2 = row2.getString(4);
						if(DateUtils.after(actionTime1, actionTime2)){
							return 1;
						}else {
							return -1;
						}
					}
				});

				/**
				 * roadFlowBuilder保存本次车辆的轨迹，是一组逗号分开的卡扣id，组合起来就是这辆车的运行轨迹
				 * 0001,0003,0005,0002
				 */
				StringBuilder roadFlowBuilder = new StringBuilder();

				/**
				 * roadFlowBuilder怎么拼起来的？  rows有顺序（按照时间），直接遍历然后追加到roadFlowBuilder就可以
				 * row.getString(1) ---- monitor_id
				 * 得到卡扣号数据，即车辆运行轨迹如0001,0002,0003,0004,0005
				 */
				for (Row row : rows) {
					roadFlowBuilder.append(","+ row.getString(1)); //卡扣号
				}
				/**
				 * roadFlowBuilder这里面的开头有一个逗号，要去掉逗号。
				 * roadFlow是本次车辆的轨迹
				 */
				String carTracker = roadFlowBuilder.toString().substring(1);
				/**
				 *  从广播变量中获取指定的卡扣流参数
				 *  0001,0002,0003,0004,0005
				 */
				String standardRoadFlow = roadFlowBroadcast.value();

				/**
				 * 对指定的卡扣流参数分割
				 * 得到{0001,0002,0003,0004,0005}
				 */
				String[] split = standardRoadFlow.split(",");

				/**
				 * [0001,0002,0003,0004,0005]
				 * 1 2 3 4 5
				 * 遍历分割完成的数组
				 */
				for (int i = 1; i <= split.length; i++) {
					//临时组成的卡扣切片  0001,0002 0001,0002,0003
					String tmpRoadFlow = "";
					/**
					 * 第一次进来：,0001
					 * 第二次进来：,0001,0002
					 * 第三次进来：,0001,0002,0003
					 * 第四次进来：,0001,0002,0003,0004
					 * 第五次进来：,0001,0002,0003,0005
					 */
					for (int j = 0; j < i; j++) {
						tmpRoadFlow += ","+split[j];//第1次——,0001   第2次——,0001,0002
					}
					tmpRoadFlow = tmpRoadFlow.substring(1); //去掉前面的逗号——0001   0001,0002

					//indexOf从哪个位置开始查找
					int index = 0;
					//这辆车有多少次匹配到这个卡扣切片的次数
					Long count = 0L;

					// https://www.cnblogs.com/xumz/p/9293434.html
					// XXX.indexOf(String str, int fromIndex)：返回str在XXX第一次出现的位置。若没有则返回-1
					while (carTracker.indexOf(tmpRoadFlow,index) != -1) { // "0001,0002,0003,0004,0005".indexOf("0001", 0)
						index = carTracker.indexOf(tmpRoadFlow,index) + 1; // 0+1
						count ++; // count变成1
					}
					list.add(new Tuple2<String, Long>(tmpRoadFlow, count)); //0001,1
				}
				return list.iterator();
			}
		});
	}

	private static Map<String, Long> getRoadFlowCount(JavaPairRDD<String, Long> roadSplitRDD) {
		JavaPairRDD<String, Long> sumByKey = roadSplitRDD.reduceByKey(new Function2<Long, Long, Long>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
		});
		 
		/**
		 * ("0001",3)
		 * ("0001,0002",2)
		 * ("0001,0002,0003",2)
		 * ("0001,0002,0003,0004",1)
		 * ("0001,0002,0003,0004,0005",0)
		 * 转换成Map出去
		 */
		// 方法一：如图所示
//		Map<String, Long> map = new HashMap<>();
//		List<Tuple2<String, Long>> results = sumByKey.collect();
//		for (Tuple2<String, Long> tuple : results) {
//			map.put(tuple._1, tuple._2);
//		}
		Map<String, Long> map = sumByKey.collectAsMap();
		return map;
	}

	/**
	 * @param roadFlowBroadcast
	 * @param roadFlow2Count
	 * @return Map<String, Double>
	 */
	private static Map<String, Double> computeRoadSplitConvertRate(String roadFlow, Map<String, Long> splitCountMap) {
		//roadFlow即0001，0002，0003，0004，0005，用来表示车流量
		String[] split = roadFlow.split(","); //split为{0001，0002，0003，0004，0005}
		//List<String> roadFlowList = Arrays.asList(split);
		/**
		 * 存放卡扣转换率
		 * "0001,0002", 0.16
		 */
		Map<String, Double> rateMap = new HashMap<>(); //存放结果
		long lastMonitorCarCount = 0L;
		String tmpRoadFlow = "";
		for (int i = 0; i < split.length; i++) {
			tmpRoadFlow += "," + split[i]; // i为0时——，0001  i为1时——，0001，0002
			//0001对应的v赋给count
			Long count = splitCountMap.get(tmpRoadFlow.substring(1));//截断第1个逗号，get(key)返回对应的v
			if(count != 0L){
				/**
				 * 1_2
				 * lastMonitorCarCount      1 count
				 */
				if(i != 0 && lastMonitorCarCount != 0L){
					double rate = NumberUtils.formatDouble((double)count/(double)lastMonitorCarCount,2); //四舍五入保留2位小数
					rateMap.put(tmpRoadFlow.substring(1), rate); // ratMap存放4个卡扣流量转换率——0001,0002，0001,0002,0003，0001到0004，0001到0005
				}
				lastMonitorCarCount = count;
			}
		}
		return rateMap;
	}

	/**
	 * car2RowRDD car   row详细信息  
	 * 按照通过时间进行排序，拿到他的轨迹  
	 * @param taskParam
	 * @param roadFlowBroadcast  ---- 0001,0002,0003,0004,0005
	 * @param car2RowRDD
	 * @return 二元组(切分的片段，该片段在本次车辆轨迹中出现的总数)
	 */



}
