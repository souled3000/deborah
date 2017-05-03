package com.vision.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class LogServiceImpl implements LogService{

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Transactional
	public void service() {
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

}
