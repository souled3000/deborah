package com.vision.main;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.vision.service.LogService;

@Component
public class Log {

	@Autowired
	private LogService logService;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public void f() throws Exception {
		String sql = "insert into his_dev_online values(19,20,'222222')";
		sql = "insert into his_dev_online values(18,20,'222222')";
		jdbcTemplate.execute(sql);
		sql = "insert into his_dev_online values(17,20,'222222')";
		jdbcTemplate.execute(sql);
		sql = "insert into his_dev_online values(15,20,'222222')";
		jdbcTemplate.execute(sql);
		sql = "insert into his_dev_online values(13,20,'222222')";
		jdbcTemplate.execute(sql);
		sql = "insert into his_dev_online values(11,20,'222222')";
		jdbcTemplate.execute(sql);
	}

	public void f2() throws Exception {
		logService.service();
	}

	private static final Logger log = LoggerFactory.getLogger(Log.class);
	private static ApplicationContext ctx;

	public static void main(String[] args) throws Exception {
		String prefix = "";
		List<String> cfg = new ArrayList<String>();
		cfg.add(prefix + "beans.xml");

		ctx = new ClassPathXmlApplicationContext(cfg.toArray(new String[] {}));
		Log l = ctx.getBean(Log.class);
		l.f();

		log.debug(ctx.getApplicationName());

		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

			public void uncaughtException(Thread t, Throwable e) {
				log.error("", e);
			}
		});
	}

}
