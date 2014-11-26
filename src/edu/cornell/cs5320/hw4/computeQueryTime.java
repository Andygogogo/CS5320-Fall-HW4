package edu.cornell.cs5320.hw4;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class computeQueryTime {
	private static String db_type;
	private static String query_type ;
	
	private static String query_traj_set_id = "005";
	private static String query_traj_name = "20081024041230.plt";
	private static String[] days = new String[]{"2009-05-09","2009-05-18","2009-05-19","2009-05-20","2009-05-21","2009-05-22","2009-05-23","2009-06-19","2009-06-20","2009-07-01"};
	//private static String[] days = new String[]{"2009-05-09","2009-05-18"};
	private static double[] resultSet;
	
	
	public static void main(String args[]){
		if (args.length != 2) {
			System.out.println("Usage: java computeQueryTime  <database catogory> <query_type> ");
			System.exit(0);
		}
		
		db_type = args[0];
		query_type = args[1];
		
		//db_type = "mongodb"; query_type = "name";
		
		if (query_type.equalsIgnoreCase("name")){
			String count_sql = String.format("SELECT count(id) FROM trajectory WHERE traj_name = \"%s\" AND traj_set_id = \"%s\"; ",query_traj_name,query_traj_set_id);
			BasicDBObject query0 = new BasicDBObject();
			query0.put("traj_set_id", query_traj_set_id);
			query0.put("traj_name", query_traj_name);
			String redis_query_key_name_format = String.format("%s:%s:*",query_traj_set_id,query_traj_name);

			if (db_type.equalsIgnoreCase("mysql")) trajectory_execute_sql_query(count_sql);
			if (db_type.equalsIgnoreCase("mongodb")) trajectory_execute_mongo_query(query0);
			if (db_type.equalsIgnoreCase("redis")) trajectory_execute_redis_key_format_query(redis_query_key_name_format);
			System.out.println("---------------------------------------------\n---------------------------------------------\n");
		}
		
		if (query_type.equalsIgnoreCase("date")){
			List<String> daysList = Arrays.asList(days);
			double[] resultTimeSet = new double[daysList.size()];
			int i = 0;
			for(String day: days){
				String count_sql = "SELECT count(id) FROM trajectory WHERE date = \"" + day + "\";"; 
				BasicDBObject query = new BasicDBObject();
				query.put("date", day);
				String redis_query_key_name_format = String.format("*:%s",day);
				if (db_type.equalsIgnoreCase("mysql")) resultTimeSet[i] = trajectory_execute_sql_query(count_sql);
				if (db_type.equalsIgnoreCase("mongodb")) resultTimeSet[i] = trajectory_execute_mongo_query(query);
				if (db_type.equalsIgnoreCase("redis")) resultTimeSet[i] = trajectory_execute_redis_key_format_query(redis_query_key_name_format);
				i++;
			}
			
			System.out.printf("The mean runtime is %f",mean(resultTimeSet));
		}
		
	}
	
	public static double mean(double[] m) {
	    double sum = 0;
	    for (int i = 0; i < m.length; i++) {
	        sum += m[i];
	    }
	    return sum / m.length;
	}
	
	private static double trajectory_execute_redis_key_format_query(String query){
		System.out.printf("Redis executing %s \n",query);

        long start = System.currentTimeMillis();
		Jedis jedis = redisHandler.getJedis();
		Set<String> names=jedis.keys(query);
		int count = names.size();
        long end = System.currentTimeMillis();

        double time_used = (end-start)/1000d;
        System.out.printf("count:%d\n",count);
        System.out.println("Redis execute time is: " + time_used + " sec\n");
        return time_used;
	}

	private static double trajectory_execute_mongo_query(BasicDBObject query){
		System.out.printf("MongoDB executing %s \n",query);

		long start = System.currentTimeMillis();
		DBCollection coll = mongoDBHandler.getCollection();
		int count = coll.find(query).count();
        long end = System.currentTimeMillis();

		double time_used = (end-start)/1000d;
		System.out.printf("count:%d\n",count);
        System.out.println("MongoDB execute time is: " + time_used + " sec\n");
        return time_used;
		
	}
	
	private static double trajectory_execute_sql_query(String sql){
		System.out.printf("MySQL executing %s \n",sql);
		
		long start = System.currentTimeMillis();
		Connection conn = mysqlHandler.getConnection();
        int count = mysqlHandler.executeQuery(conn, sql);
        long end = System.currentTimeMillis();
        
        double time_used = (end-start)/1000d;
        System.out.printf("count:%d\n",count);
        System.out.println("MySQL execute time is: " + time_used + " sec\n");
        return time_used;
	}
	
	
}
