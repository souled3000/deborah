package com.vision.executor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.jdo.annotations.Transactional;

import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.vision.util.Constants;
import com.vision.util.HiveTemplate;
@Component
public class E1 {
	
	private static final Logger log = LoggerFactory.getLogger(E1.class);
	private Map<String, String> m;
	private Map<String, String> day;
	private Map<String, String> month;
	private Map<String, String> year;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	// @Autowired
	private HiveTemplate hiveTemplate;

	private String dt;

	private List<Object[]> statistic;

	private Date begin;
	
	private void init() {
		begin=new Date();
		Date lastHour = DateUtils.addHours(begin, -1);
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMddHH");
		dt = sdf.format(lastHour);
		log.info("This time is {}",dt);
	}

	/**
	 * 第一步 导入分区元数据
	 */
	private void step1() {
		String hql = m.get("step1").replaceAll(Constants.TOKEN, dt);
		try {
			hiveTemplate.exec(hql);
		} catch (Exception e) {
			e.printStackTrace();
			step1();
		}
	}

	/**
	 * 第二步 统计分析
	 */
	private void step2() {
		String hql = m.get("step2").replaceAll(Constants.TOKEN, dt);
		try {
			this.statistic = hiveTemplate.query4LO(hql);
		} catch (Exception e) {
			e.printStackTrace();
			step2();
		}
	}

	/**
	 * 第三步 统计结果入mysql
	 */
	@Transactional
	private void step3() {
		int[] batchok = jdbcTemplate.batchUpdate(this.m.get("step3"), this.statistic);
		log.debug("inserted {} rows into his_dev.", batchok.length);
	}

//	@Scheduled(cron = "0 0/6 * * * ?")
//	@Scheduled(cron = "0 30 * * * ?")
	public void go() {
		init();
		try {
			step1();
			step2();
			step3();
			long last = System.currentTimeMillis()-this.begin.getTime();
			log.info("EVENTUALLY: {} lasted {} second", this.dt ,last/1000);
		} catch (Exception e) {
			log.error("",e);
		}
	}
	
	/**每天1点30分，统计昨天*/
	@Scheduled(cron = "0 0/6 * * * ?")
//	@Scheduled(cron = "0 0 1 * * ?")
	public void day() {
		begin=new Date();
		Date lastDay = DateUtils.addDays(begin, -1);
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
		dt = sdf.format(lastDay);
		log.info("This time is {}",dt);
		
		List<Object[]> statistic=null;
		try {
			String hql = this.day.get("step1").replaceAll(Constants.TOKEN, dt);
			try {
				statistic = hiveTemplate.query4LO(hql);
			} catch (Exception e) {
				e.printStackTrace();
				day();
			}
			
			if (statistic !=null){
				int[] batchok = jdbcTemplate.batchUpdate(this.day.get("step2"), statistic);
				log.debug("inserted {} rows into his_dev_day.", batchok.length);
			}
			long last = System.currentTimeMillis()-this.begin.getTime();
			log.info("EVENTUALLY: {} lasted {} second", dt ,last/1000);
		} catch (Exception e) {
			log.error("",e);
		}
	}
	
	/**每月第一天2点半，统计上个月*/
	@Scheduled(cron = "0 0 2 1 * ?")
	public void month() {
		begin=new Date();
		Date lastMonth = DateUtils.addMonths(begin, -1);
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMM");
		dt = sdf.format(lastMonth);
		log.info("This time is {}",dt);
		
		List<Object[]> statistic=null;
		try {
			String hql = this.month.get("step1").replaceAll(Constants.TOKEN, dt);
			try {
				statistic = hiveTemplate.query4LO(hql);
			} catch (Exception e) {
				e.printStackTrace();
				month();
			}
			
			if (statistic !=null){
				int[] batchok = jdbcTemplate.batchUpdate(this.month.get("step2"), statistic);
				log.debug("inserted {} rows into his_dev_month.", batchok.length);
			}
			long last = System.currentTimeMillis()-this.begin.getTime();
			log.info("EVENTUALLY: {} lasted {} second", dt ,last/1000);
		} catch (Exception e) {
			log.error("",e);
		}
	}
	
	/**每年1月1日3点半，统计上一年*/
	@Scheduled(cron = "0 0 3 1 1 ?")
	public void year() {
		begin=new Date();
		Date lastYear = DateUtils.addYears(begin, -1);
		SimpleDateFormat sdf = new SimpleDateFormat("YYYY");
		dt = sdf.format(lastYear);
		log.info("This time is {}",dt);
		
		List<Object[]> statistic=null;
		try {
			String hql = this.year.get("step1").replaceAll(Constants.TOKEN, dt);
			try {
				statistic = hiveTemplate.query4LO(hql);
			} catch (Exception e) {
				e.printStackTrace();
				year();
			}
			
			if (statistic !=null){
				int[] batchok = jdbcTemplate.batchUpdate(this.year.get("step2"), statistic);
				log.debug("inserted {} rows into his_dev_year.", batchok.length);
			}
			long last = System.currentTimeMillis()-this.begin.getTime();
			log.info("EVENTUALLY: {} lasted {} second", dt ,last/1000);
		} catch (Exception e) {
			log.error("",e);
		}
	}

	public void setM(Map<String, String> m) {
		this.m = m;
	}

	public void setHiveTemplate(HiveTemplate hiveTemplate) {
		this.hiveTemplate = hiveTemplate;
	}

	public void setDay(Map<String, String> day) {
		this.day = day;
	}

	public void setMonth(Map<String, String> month) {
		this.month = month;
	}

	public void setYear(Map<String, String> year) {
		this.year = year;
	}
}
