set hive.support.sql11.reserved.keywords=false; // 关闭关键字检查，因为第3行的date就是关键字

CREATE TABLE IF NOT EXISTS traffic.monitor_flow_action(
date string ,
monitor_id string ,
camera_id string ,
car string ,
action_time string ,
speed string  ,
road_id string,
area_id string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;

load data local inpath '/test/monitor_flow_action' into table traffic.monitor_flow_action;

CREATE TABLE IF NOT EXISTS traffic.monitor_camera_info(
monitor_id string ,
camera_id string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;

load data local inpath '/test/monitor_camera_info' into table traffic.monitor_camera_info;
