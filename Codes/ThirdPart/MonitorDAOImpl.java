package com.bjsxt.spark.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.bjsxt.spark.dao.IMonitorDAO;
import com.bjsxt.spark.domain.MonitorState;
import com.bjsxt.spark.domain.TopNMonitor2CarCount;
import com.bjsxt.spark.domain.TopNMonitorDetailInfo;
import com.bjsxt.spark.jdbc.JDBCHelper;

/**
 * 卡口流量监控管理DAO接口实现类
 * @author root
 *
 */

public class MonitorDAOImpl implements IMonitorDAO {

	@Override
	//向数据库表topn_monitor_car_count中插入车流量最多的TopN数据
	public void insertBatchTopN(List<TopNMonitor2CarCount> topNMonitor2CarCounts) {
		 JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		 String sql = "INSERT INTO topn_monitor_car_count VALUES(?,?,?)";
		 List<Object[]> params = new ArrayList<>();
		 for (TopNMonitor2CarCount topNMonitor2CarCount : topNMonitor2CarCounts) {
			 params.add(new Object[]{topNMonitor2CarCount.getTaskId(),topNMonitor2CarCount.getMonitorId(),topNMonitor2CarCount.getCarCount()});
		}
		jdbcHelper.executeBatch(sql , params);
	}

	@Override
	//将topN的卡扣车流量明细数据存入topn_monitor_detail_info表中
	public void insertBatchMonitorDetails(List<TopNMonitorDetailInfo> monitorDetailInfos) {
		 JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		 String sql = "INSERT INTO topn_monitor_detail_info VALUES(?,?,?,?,?,?,?,?)";
		 List<Object[]> params = new ArrayList<>();
		 for(TopNMonitorDetailInfo m : monitorDetailInfos){
			 params.add(new Object[]{m.getTaskId(),m.getDate(),m.getMonitorId(),m.getCameraId(),m.getCar(),m.getActionTime(),m.getSpeed(),m.getRoadId()});
		 }
		 jdbcHelper.executeBatch(sql, params);
	}

	@Override
	//向数据库表monitor_state中添加累加器累计的各个值
	public void insertMonitorState(MonitorState monitorState) {
		 JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		 String sql = "INSERT INTO monitor_state VALUES(?,?,?,?,?,?)";
		 Object[] param = new Object[]{
				 monitorState.getTaskId(),
				 monitorState.getNormalMonitorCount(),
				 monitorState.getNormalCameraCount(),
				 monitorState.getAbnormalMonitorCount(),
				 monitorState.getAbnormalCameraCount(),
				 monitorState.getAbnormalMonitorCameraInfos()};
		 List<Object[]> params = new ArrayList<>();
		 params.add(param);
		 jdbcHelper.executeBatch(sql, params);
	}
	//向数据表top10_speed_detail插入车流量明细数据共计8个字段
	@Override
	public void insertBatchTop10Details(List<TopNMonitorDetailInfo> topNMonitorDetailInfos) {
		 JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		 String sql = "INSERT INTO top10_speed_detail VALUES(?,?,?,?,?,?,?,?)";
		 List<Object[]> params = new ArrayList<>();
		 for(TopNMonitorDetailInfo m : topNMonitorDetailInfos){
			 params.add(new Object[]{m.getTaskId(),m.getDate(),m.getMonitorId(),m.getCameraId(),m.getCar(),m.getActionTime(),m.getSpeed(),m.getRoadId()});
		 }
		 jdbcHelper.executeBatch(sql, params);
	}

 
}
