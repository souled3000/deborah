package com.vision.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hive.jdbc.HiveDriver;

public class HiveDemo {

	public final static String url = "jdbc:hive2://193.168.1.115:10000/default";
	public final static String username = "sophia";
	public final static String password = "lchj";
	public final static String driverClass = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) throws Exception {
		f3();
	}

	public static void f3() throws Exception {
		Class.forName(driverClass);
		Connection con = DriverManager.getConnection(url, username, password);
		String sql = "LOAD DATA LOCAL INPATH '/home/sophia/a' OVERWRITE INTO TABLE his_dev PARTITION (dt='20080808')";
		PreparedStatement sta = con.prepareStatement(sql);
		sta.execute();
		sta.close();
		con.close();
	}
	
	public static void f2() throws Exception {
		Class.forName(driverClass);
		Connection con = DriverManager.getConnection(url, username, password);
		String sql = "select mac from his_dev";
		PreparedStatement sta = con.prepareStatement(sql);
		ResultSet rs = sta.executeQuery();
		while (rs != null && rs.next()) {
			String name = rs.getString(1);
			System.out.println(name);
		}
		rs.close();
		sta.close();
		con.close();
	}

	public static void f1() throws Exception {
		Class.forName(driverClass);
//		DriverManager.registerDriver(new HiveDriver());
		Connection con = DriverManager.getConnection(url,username, password);
		Statement stmt = con.createStatement();
//		stmt.execute("insert into table hb partition (dt = '2016091317') select id,mac,ts,dv  from hb where id = 1");
		stmt.execute("INSERT INTO TABLE his_dev PARTITION (dt = '2016091317') SELECT id,mac,dv,lt,ts  FROM his_dev WHERE dt = '1'");
		stmt.close();
		con.close();
	}

	public static void f() throws Exception {
		DriverManager.registerDriver(new HiveDriver());
		Connection con = DriverManager.getConnection(url ,username, password);
		Statement stmt = con.createStatement();
		String tableName = "t";

		stmt.execute("drop table " + tableName);

		ResultSet res = null;
		stmt.execute("create table " + tableName + " (id string, name string) row format delimited fields terminated by '#' stored as textfile");

		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// load data into table
		// NOTE: filepath has to be local to the hive server
		// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
		String filepath = "/home/sophia/t";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running: " + sql);
		// res = stmt.executeQuery(sql);

		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getString(1)) + "\t" + res.getString(2));
		}

		// regular hive query
		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}
}
