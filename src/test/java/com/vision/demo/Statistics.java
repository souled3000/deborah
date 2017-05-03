package com.vision.demo;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;

public class Statistics {

	final static String PARTITIONQL = "INSERT INTO TABLE hb PARTITION (dt = 'TOKEN') SELECT id,mac,ts,dv  FROM hb WHERE id = 1";
	final static String COUNTINGQL = 	"SELECT dv,n,'TOKEN' dt"+
										"FROM"+
										"(SELECT dv,COUNT(DISTINCT(id)) n FROM hb WHERE dt = 'TOKEN' AND lt = 2 GROUP BY dv"+
										"UNION"+
										"SELECT 0 dv,COUNT(DISTINCT(id)) n FROM hb WHERE dt = 'TOKEN' AND lt = 2)t ORDER BY dv";
	final static String RTQL = "INSERT INTO TABLE his_dev_online (dv,amount,dt) values (?,?,?)";
	public static void main(String[] args) {
		Date lastHour = DateUtils.addHours(new Date(), -1);
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMddHH");
		String dt = sdf.format(lastHour);
		String partitionql = PARTITIONQL.replaceAll("TOKEN", dt);
		System.out.println(partitionql);
	}

	public static void f() throws Exception{
		//生成上一个小时
		Date lastHour = DateUtils.addHours(new Date(), -1);
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMddHH");
		String dt = sdf.format(lastHour);
		String partitionql = PARTITIONQL.replaceAll("TOKEN", dt); 
		//生成上一个小时的hive分区meta
		
		//执行统计hivesql，将结果插入mysql表
		
	
	}
}
