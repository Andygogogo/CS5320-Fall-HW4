package edu.cornell.cs5320.hw4;
import java.io.*;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import com.mongodb.DBCollection;

import java.util.Date;

import redis.clients.jedis.Jedis;

public class importData {
	private static String data_dir = "./Geolife Trajectories 1.1/Data/";
	
	private static boolean buildSql = false;
	private static boolean buildMongo = false;
	private static boolean buildRedis = false;
	
	public static void main(String[] args){
		if (args.length != 2) {
			System.out.println("Usage: java <database catogory> <trajecotry data directory>");
			System.exit(0);
		}
		data_dir = args[1];
		if(args[0].equalsIgnoreCase("mysql")) buildSql = true;
		if (args[0].equalsIgnoreCase("mongodb")) buildMongo = true;
		if (args[0].equalsIgnoreCase("redis")) buildRedis = true;
		
		buildDatabase();
	}
	
	public static void buildDatabase(){
		if(buildSql)    mysqlHandler.createDB();
		if(buildMongo)  mongoDBHandler.clearCollection();
		if(buildRedis)  redisHandler.clearData();
		
		readFiles(data_dir);
	}
	
	public static void readFiles(String data_dir) {
		//calculate time used
		Date time1 = new Date();
		
		File[] files = new File(data_dir).listFiles();
		

		//initialize conn
		Connection conn =  mysqlHandler.getConnection();
		Statement stmt = mysqlHandler.createStatement(conn);
		//initialize coll
		DBCollection coll = mongoDBHandler.getCollection();
		//initialize jedis
		Jedis jedis = redisHandler.getJedis();

		BufferedReader br = null;
		String currentLine;
		String traj_set_id, traj_dir, traj_name, traj_loc;
		int currentLineNumber;
		trajectory t ;
		
//		int k1 =1;
		for (File file : files) {
//			k1++;if(k1>3) break;
			if(file.getName().startsWith(".")) continue;
			traj_set_id = file.getName();
			traj_dir = data_dir+traj_set_id+"/Trajectory/";
			
			File[] traj_files = new File(traj_dir).listFiles();
//			int k = 0;
			System.out.println("Processing "+traj_dir);
			for (File traj_file : traj_files){
//				k++;if(k>2) break;
				if(traj_file.getName().startsWith(".")) continue;
				//read file content
				traj_name = traj_file.getName();
				traj_loc = traj_file.getPath();
				System.out.println("handling "+traj_loc);
				try{
					br = new BufferedReader(new FileReader(traj_loc));
					currentLineNumber = 0;

					while((currentLine = br.readLine()) != null ){
						currentLineNumber++;
						if (currentLineNumber <=6)continue; //discard information lines
//						System.out.println(currentLine);
						t = new trajectory(currentLine);

						if(buildSql)  mysqlHandler.inesertTrajItem(conn, traj_set_id, traj_name, t);
//						    insertTrajToSql(conn,traj_set_id,traj_name,currentLine);
						if(buildMongo)  mongoDBHandler.inesertTrajItem(coll, traj_set_id, traj_name, t);
//							insertTrajToMongo(coll,traj_set_id,traj_name,currentLine);
						if(buildRedis)  redisHandler.inesertTrajItem(jedis, currentLineNumber, traj_set_id, traj_name, t);
					}
					System.out.println(currentLineNumber);
					br.close();
				}
				catch (Exception e){
					e.printStackTrace();
				}
			}
		}
		
		Date time2 = new Date();
		double t_diff = (time2.getTime()-time1.getTime())/1000.0;
		System.out.println("Finished inserting!, Inserting time Used :"+ t_diff +"seconds!");

		try {
			conn.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
