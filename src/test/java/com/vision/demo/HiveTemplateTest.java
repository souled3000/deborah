package com.vision.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import com.vision.util.HiveTemplate;

public class HiveTemplateTest {

	static String driverClassName="org.apache.hive.jdbc.HiveDriver";
	static String url="jdbc:hive2://193.168.1.123:10000/default";
	static String username="sophia";
	static String password="lchj";
	final static String COUNTINGQL = 	"SELECT dv,n,'TOKEN' dt "+
			"FROM "+
			"(SELECT dv,COUNT(DISTINCT(id)) n FROM hb WHERE dt = 'TOKEN' AND lt = 2 GROUP BY dv "+
			"UNION "+
			"SELECT 0 dv,COUNT(DISTINCT(id)) n FROM hb WHERE dt = 'TOKEN' AND lt = 2)t ORDER BY dv";
	public static void main(String[] args) throws Exception {
		testExec();
//		test4LO();
	}
	
	public static void f1() throws Exception {
		Class.forName("org.apache.hive.jdbc.HiveDriver");
//		DriverManager.registerDriver(new HiveDriver());
		Connection con = DriverManager.getConnection("jdbc:hive2://193.168.1.123:10000/default","sophia", "lchj");
		Statement stmt = con.createStatement();
//		stmt.execute("insert into table hb partition (dt = '2016091317') select id,mac,ts,dv  from hb where id = 1");
		stmt.execute("INSERT INTO TABLE his_dev PARTITION (dt = '2016091317') SELECT id,mac,dv,lt,ts  FROM his_dev WHERE dt = '1'");
		stmt.close();
		con.close();
	}
	public static void testExec()throws Exception{
		HiveTemplate o = new HiveTemplate();
		o.setDriverClassName(driverClassName);
		o.setUrl(url);
		o.setUsername(username);
		o.setPassword(password);
		String hql ="INSERT INTO TABLE his_dev PARTITION (dt = '2016091317') SELECT id,mac,dv,lt,ts  FROM his_dev WHERE dt = '1'";
		o.exec(hql);
	}
	public static void test4LM()throws Exception{
		HiveTemplate o = new HiveTemplate();
		o.setDriverClassName(driverClassName);
		o.setUrl(url);
		o.setUsername(username);
		o.setPassword(password);
		String hql = COUNTINGQL.replace("TOKEN", "2016091510");
		List<Map<String,Object>> r=o.query4LM(hql);
		for(Map<String,Object> e:r ){
			for(Map.Entry<String, Object> i:e.entrySet()){
				System.out.println(i.getKey()+":"+i.getValue().toString());
			}
			System.out.println("--------------------------------------");
		}
	}
	
	public static void test4LO()throws Exception{
		HiveTemplate o = new HiveTemplate();
		o.setDriverClassName(driverClassName);
		o.setUrl(url);
		o.setUsername(username);
		o.setPassword(password);
		String hql = COUNTINGQL.replace("TOKEN", "2016091510");
		List<Object[]> r=o.query4LO(hql);
		System.out.println("--------------------------------------");
		for(Object[] e:r ){
			for(Object i:e){
				System.out.println(i);
			}
			System.out.println("--------------------------------------");
		}
	}

}
