package com.vision.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveTemplate {

	private static final Logger log = LoggerFactory.getLogger(HiveTemplate.class);
	private String driverClassName;
	private String url;
	private String username;
	private String password;
	static {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	private Connection con;

	private Connection connect() throws Exception {
		log.info("CONNECT HIVE");
		if (con == null || con.isClosed()) {
			this.con = DriverManager.getConnection(url, username, password);
		}
		return this.con;
	}

	public void exec(String hql) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = connect();
			stmt = conn.createStatement();
			stmt.execute(hql);
		} catch (SQLException e) {
			throw new Exception("query hql [" + hql + "] is error", e);
		} finally {
			closeDB(rs, stmt, conn);
		}
	}

	public List<Map<String, Object>> query4LM(String hql) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = connect();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(hql); // 执行查询语句
			ResultSetMetaData meta = rs.getMetaData();
			List<Map<String, Object>> listValue = new ArrayList<Map<String, Object>>();
			Map<String, Class> map = new HashMap<String, Class>();
			int type = 0;
			Class zlass = null;
			for (int i = 1; i <= meta.getColumnCount(); i++) {
				type = meta.getColumnType(i);
				if (type == Types.INTEGER || type == Types.BIGINT || type == Types.NUMERIC) {
					zlass = Long.class;
				} else if (type == Types.FLOAT || type == Types.DOUBLE) {
					zlass = Double.class;
				} else if (type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP) {
					zlass = Date.class;
				} else {
					zlass = String.class;
				}
				String name = meta.getColumnName(i);
				map.put(name, zlass);
			}
			Map<String, Object> valueMap = null;
			Set<String> set = map.keySet();
			Object o = null;
			Object temp = null;
			while (rs.next()) {
				valueMap = new HashMap<String, Object>();
				for (String key : set) {
					if (map.get(key) == Date.class) {
						o = rs.getDate(key);
					} else if (map.get(key) == Long.class) {
						o = rs.getLong(key);
					} else if (map.get(key) == Double.class) {
						o = rs.getDouble(key);
					} else {
						temp = rs.getObject(key);
						o = (temp == null ? null : temp.toString());
					}
					valueMap.put(key.toUpperCase(), o);
				}
				listValue.add(valueMap);
			}
			return listValue;
		} catch (SQLException e) {
			throw new Exception("query hql [" + hql + "] is error", e);
		} finally {
			closeDB(rs, stmt, conn);
		}
	}

	public List<Object[]> query4LO(String hql) throws Exception {
		Statement stmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = connect();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(hql); // 执行查询语句
			ResultSetMetaData meta = rs.getMetaData();
			List<Object[]> listValue = new ArrayList<Object[]>();
			Map<String, Class> map = new HashMap<String, Class>();
			List<String> columns = new ArrayList<String>();
			int type = 0;
			Class zlass = null;
			for (int i = 1; i <= meta.getColumnCount(); i++) {
				type = meta.getColumnType(i);
				if (type == Types.INTEGER || type == Types.BIGINT || type == Types.NUMERIC) {
					zlass = Long.class;
				} else if (type == Types.FLOAT || type == Types.DOUBLE) {
					zlass = Double.class;
				} else if (type == Types.DATE || type == Types.TIME || type == Types.TIMESTAMP) {
					zlass = Date.class;
				} else {
					zlass = String.class;
				}
				String name = meta.getColumnName(i);
				columns.add(name);
				map.put(name, zlass);
			}
			Object o = null;
			Object temp = null;
			while (rs.next()) {
				Object[] row = new Object[columns.size()];
				int i = 0;
				for (String key : columns) {
					if (map.get(key) == Date.class) {
						o = rs.getDate(key);
					} else if (map.get(key) == Long.class) {
						o = rs.getLong(key);
					} else if (map.get(key) == Double.class) {
						o = rs.getDouble(key);
					} else {
						temp = rs.getObject(key);
						o = (temp == null ? null : temp.toString());
					}
					row[i++] = o;
				}
				listValue.add(row);
			}
			return listValue;
		} catch (SQLException e) {
			throw new Exception("query hql [" + hql + "] is error", e);
		} finally {
			closeDB(rs, stmt, conn);
		}
	}

	private void closeDB(ResultSet rs, Statement stmt, Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				log.error("", e);
			}
		}
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				log.error("", e);
			}
		}
		// if (conn != null) {
		// try {
		// conn.close();
		// } catch (SQLException e) {
		// log.error("", e);
		// }
		// }
		log.info("CLOSE HIVE");
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}
}
